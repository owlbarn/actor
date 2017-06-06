(** [ Data Parallel ] Map-Reduce server module *)

open Actor_types

(* the global context: master, worker, etc. *)
let _context = ref (Actor_utils.empty_mapre_context ())

let barrier bar = Actor_barrier.mapre_bsp bar _context

let _broadcast_all t s =
  let bar = Random.int 536870912 in
  StrMap.iter (fun k v -> Actor_utils.send ~bar v t s) !_context.workers;
  bar

let run_job_eager () =
  List.iter (fun s ->
    let s' = List.map (fun x -> Actor_dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = barrier bar in
    Actor_dag.mark_stage_done s;
  ) (Actor_dag.stages_eager ())

let run_job_lazy x =
  List.iter (fun s ->
    let s' = List.map (fun x -> Actor_dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = barrier bar in
    Actor_dag.mark_stage_done s;
  ) (Actor_dag.stages_lazy x)

let collect x =
  Actor_logger.info "%s" ("collect " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Collect [|x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)

let count x =
  Actor_logger.info "%s" ("count " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Count [|x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.fold_left (+) 0

let fold f a x =
  Actor_logger.info "%s" ("fold " ^ x ^ "\n");
  run_job_lazy x;
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let bar = _broadcast_all Fold [|g; x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.filter (function Some x -> true | None -> false)
  |> List.map (function Some x -> x | None -> failwith "")
  |> List.fold_left f a

let reduce f x =
  Actor_logger.info "%s" ("reduce " ^ x ^ "\n");
  run_job_lazy x;
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let bar = _broadcast_all Reduce [|g; x|] in
  let y = barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.filter (function Some x -> true | None -> false)
  |> List.map (function Some x -> x | None -> failwith "") in
  match y with
  | hd :: tl -> Some (List.fold_left f hd tl)
  | [] -> None

let terminate () =
  Actor_logger.info "%s" ("terminate #" ^ !_context.job_id ^ "\n");
  let bar = _broadcast_all Terminate [||] in
  let _ = barrier bar in ()

let broadcast x =
  Actor_logger.info "%s" ("broadcast -> " ^ string_of_int (StrMap.cardinal !_context.workers) ^ " workers\n");
  let y = Actor_memory.rand_id () in
  let bar = _broadcast_all Broadcast [|Marshal.to_string x []; y|] in
  let _ = barrier bar in y

let get_value x = Actor_memory.find x

let map f x =
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("map " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Actor_dag.add_edge (to_msg 0 MapTask [|g; x; y|]) x y Red; y

let map_partition f x =
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("map_partition " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Actor_dag.add_edge (to_msg 0 MapPartTask [|g; x; y|]) x y Red; y

let filter f x =
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("filter " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Actor_dag.add_edge (to_msg 0 FilterTask [|g; x; y|]) x y Red; y

let flatten x =
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("flatten " ^ x ^ " -> " ^ y ^ "\n");
  Actor_dag.add_edge (to_msg 0 FlattenTask [|x; y|]) x y Red; y

let flatmap f x = flatten (map f x)

let union x y =
  let z = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("union " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  Actor_dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) x z Red;
  Actor_dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) y z Red; z

let shuffle x =
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("shuffle " ^ x ^ " -> " ^ y ^ "\n");
  let z = Marshal.to_string (StrMap.keys !_context.workers) [] in
  let b = Marshal.to_string (Random.int 536870912) [] in
  Actor_dag.add_edge (to_msg 0 ShuffleTask [|x; y; z; b|]) x y Blue; y

let reduce_by_key f x =
  (* TODO: without local combiner ... keep or not? *)
  let x = shuffle x in
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("reduce_by_key " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Actor_dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red; y

let ___reduce_by_key f x =
  (* TODO: with local combiner ... keep or not? *)
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let y = Actor_memory.rand_id () in
  Actor_dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red;
  let x = shuffle y in
  let y = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("reduce_by_key " ^ x ^ " -> " ^ y ^ "\n");
  Actor_dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red; y

let join x y =
  let z = Actor_memory.rand_id () in
  Actor_logger.info "%s" ("join " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  let x, y = shuffle x, shuffle y in
  Actor_dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) x z Red;
  Actor_dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) y z Red; z

let apply f i o =
  Actor_logger.info "%s" ("apply f ... " ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let o = List.map (fun _ -> Actor_memory.rand_id ()) o in
  let x = Marshal.to_string i [ ] in
  let y = Marshal.to_string o [ ] in
  let z = Actor_memory.rand_id () in
  List.iter (fun m -> Actor_dag.add_edge (to_msg 0 ApplyTask [|g; x; z; y|]) m z Red) i;
  List.iter (fun n -> Actor_dag.add_edge (to_msg 0 NopTask [|z; y|]) z n Red) o; o

let load x =
  Actor_logger.info "%s" ("load " ^ x ^ "\n");
  let y = Actor_memory.rand_id () in
  let bar = _broadcast_all Load [|x; y|] in
  let _ = barrier bar in y

let save x y =
  Actor_logger.info "%s" ("save " ^ x ^ "\n");
  let bar = _broadcast_all Save [|x; y|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.fold_left (+) 0

let init m context =
  _context := context;
  (* contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.map (fun x ->
    let req = ZMQ.Socket.create !_context.ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    Actor_utils.send req Job_Create [|!_context.myself_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (* wait until all the allocated actors register *)
  while (StrMap.cardinal !_context.workers) < (List.length addrs) do
    let i, m = Actor_utils.recv !_context.myself_sock in
    let s = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m.par.(0);
    !_context.workers <- (StrMap.add m.par.(0) s !_context.workers);
  done


(* experimental functions *)

let workers () = StrMap.keys !_context.workers


(* ends here *)
