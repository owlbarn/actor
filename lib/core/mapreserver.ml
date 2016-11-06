(** [ Data Parallel ] Service module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())
let _msgbuf = Hashtbl.create 1024

let barrier bar = Barrier.bsp bar !_context.myself_sock !_context.workers _msgbuf

let _broadcast_all t s =
  let bar = Random.int 536870912 in
  StrMap.iter (fun k v -> Utils.send ~bar v t s) !_context.workers;
  bar

let run_job_eager () =
  List.iter (fun s ->
    let s' = List.map (fun x -> Dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = barrier bar in
    Dag.mark_stage_done s;
  ) (Dag.stages_eager ())

let run_job_lazy x =
  List.iter (fun s ->
    let s' = List.map (fun x -> Dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = barrier bar in
    Dag.mark_stage_done s;
  ) (Dag.stages_lazy x)

let collect x =
  Logger.info "%s" ("collect " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Collect [|x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)

let count x =
  Logger.info "%s" ("count " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Count [|x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.fold_left (+) 0

let fold f a x =
  Logger.info "%s" ("fold " ^ x ^ "\n");
  run_job_lazy x;
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let bar = _broadcast_all Fold [|g; x|] in
  barrier bar
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.filter (function Some x -> true | None -> false)
  |> List.map (function Some x -> x | None -> failwith "")
  |> List.fold_left f a

let reduce f x =
  Logger.info "%s" ("reduce " ^ x ^ "\n");
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
  Logger.info "%s" ("terminate #" ^ !_context.job_id ^ "\n");
  let bar = _broadcast_all Terminate [||] in
  let _ = barrier bar in ()

let broadcast x =
  Logger.info "%s" ("broadcast -> " ^ string_of_int (StrMap.cardinal !_context.workers) ^ " workers\n");
  let y = Memory.rand_id () in
  let bar = _broadcast_all Broadcast [|Marshal.to_string x []; y|] in
  let _ = barrier bar in y

let get_value x = Memory.find x

let map f x =
  let y = Memory.rand_id () in
  Logger.info "%s" ("map " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 MapTask [|g; x; y|]) x y Red; y

let filter f x =
  let y = Memory.rand_id () in
  Logger.info "%s" ("filter " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 FilterTask [|g; x; y|]) x y Red; y

let flatten x =
  let y = Memory.rand_id () in
  Logger.info "%s" ("flatten " ^ x ^ " -> " ^ y ^ "\n");
  Dag.add_edge (to_msg 0 FlattenTask [|x; y|]) x y Red; y

let flatmap f x = flatten (map f x)

let union x y =
  let z = Memory.rand_id () in
  Logger.info "%s" ("union " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  Dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) x z Red;
  Dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) y z Red; z

let shuffle x =
  let y = Memory.rand_id () in
  Logger.info "%s" ("shuffle " ^ x ^ " -> " ^ y ^ "\n");
  let z = Marshal.to_string (StrMap.keys !_context.workers) [] in
  let b = Marshal.to_string (Random.int 536870912) [] in
  Dag.add_edge (to_msg 0 ShuffleTask [|x; y; z; b|]) x y Blue; y

let reduce_by_key f x =
  (* TODO: without local combiner ... keep or not? *)
  let x = shuffle x in
  let y = Memory.rand_id () in
  Logger.info "%s" ("reduce_by_key " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red; y

let ___reduce_by_key f x =
  (* TODO: with local combiner ... keep or not? *)
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let y = Memory.rand_id () in
  Dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red;
  let x = shuffle y in
  let y = Memory.rand_id () in
  Logger.info "%s" ("reduce_by_key " ^ x ^ " -> " ^ y ^ "\n");
  Dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red; y

let join x y =
  let z = Memory.rand_id () in
  Logger.info "%s" ("join " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  let x, y = shuffle x, shuffle y in
  Dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) x z Red;
  Dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) y z Red; z

let apply f i o =
  Logger.info "%s" ("apply f ... " ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let o = List.map (fun _ -> Memory.rand_id ()) o in
  let x = Marshal.to_string i [ ] in
  let y = Marshal.to_string o [ ] in
  let z = Memory.rand_id () in
  List.iter (fun m -> Dag.add_edge (to_msg 0 ApplyTask [|g; x; z; y|]) m z Red) i;
  List.iter (fun n -> Dag.add_edge (to_msg 0 NopTask [|z; y|]) z n Red) o; o

let load x =
  Logger.info "%s" ("load " ^ x ^ "\n");
  let y = Memory.rand_id () in
  let bar = _broadcast_all Load [|x; y|] in
  let _ = barrier bar in y

let save x y =
  Logger.info "%s" ("save " ^ x ^ "\n");
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
    Utils.send req Job_Create [|!_context.myself_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (* wait until all the allocated actors register *)
  while (StrMap.cardinal !_context.workers) < (List.length addrs) do
    let i, m = Utils.recv !_context.myself_sock in
    let s = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m.par.(0);
    !_context.workers <- (StrMap.add m.par.(0) s !_context.workers);
  done
