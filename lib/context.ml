(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  mutable jid : string;
  mutable master : string;
  mutable worker : [`Dealer] ZMQ.Socket.t StrMap.t;
}

(** some global varibles *)
let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _ztx = ZMQ.Context.create ()
let _addr, _router = Utils.bind_available_addr _ztx
let _msgbuf = Hashtbl.create 1024

(** update config information *)
let _ = Config.(update_logger "" level)

let barrier bar = Barrier.bsp bar _router _context.worker _msgbuf

let dbp_bar bar = Barrier.dbp bar _router _context.worker _msgbuf

let _broadcast_all t s =
  let bar = Random.int 536870912 in
  StrMap.iter (fun k v -> Utils.send ~bar v t s) _context.worker;
  bar

let shuffle bar x z =
  List.mapi (fun i k ->
    let v = Utils.choose_load x (List.length z) i in
    let s = if StrMap.mem k _context.worker then
      StrMap.find k _context.worker
    else (
      let s = ZMQ.Socket.(create _ztx dealer) in
      let _ = ZMQ.Socket.(set_identity s _addr; connect s k) in
      let _ = _context.worker <- StrMap.add k s _context.worker in
      s ) in
    Utils.send ~bar s OK [|Marshal.to_string v []|]
  ) z

let process_pipeline s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
      Logger.info "%s" ("map @ " ^ _addr);
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      List.map f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FilterTask -> (
      Logger.info "%s" ("filter @ " ^ _addr);
      let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
      List.filter f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FlattenTask -> (
      Logger.info "%s" ("flatten @ " ^ _addr);
      List.flatten (Memory.find m.par.(0)) |> Memory.add m.par.(1)
      )
    | UnionTask -> (
      Logger.info "%s" ("union @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Memory.add m.par.(2)
      )
    | ReduceByKeyTask -> (
      Logger.info "%s" ("reduce_by_key @ " ^ _addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      Memory.find m.par.(1) |> Utils.group_by_key |> List.map (fun (k,l) ->
        match l with
        | hd :: tl -> (k, List.fold_left f hd tl)
        | [] -> failwith "error in reduce"
      ) |> Memory.add m.par.(2)
      )
    | JoinTask -> (
      Logger.info "%s" ("join @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Utils.group_by_key |> Memory.add m.par.(2)
      )
    | ShuffleTask -> (
      Logger.info "%s" ("shuffle @ " ^ _addr);
      let x = Memory.find m.par.(0) |> Utils.group_by_key in
      let z = Marshal.from_string m.par.(2) 0 in
      let bar = Marshal.from_string m.par.(3) 0 in
      let _ = shuffle bar x z in
      barrier bar
      |> List.map (fun m -> Marshal.from_string m.par.(0) 0 |> Utils.flatten_kvg)
      |> List.flatten |> Memory.add m.par.(1);
      )
    | _ -> Logger.info "%s" "unknown task types"
  ) s

let master_fun m =
  _context.master <- _addr;
  (* contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.map (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    Utils.send req Job_Create [|_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (* wait until all the allocated actors register *)
  while (StrMap.cardinal _context.worker) < (List.length addrs) do
    let i, m = Utils.recv _router in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m.par.(0);
    _context.worker <- (StrMap.add m.par.(0) s _context.worker);
  done

let worker_fun m =
  _context.master <- m.par.(0);
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master _context.master;
  Utils.send master OK [|_addr|];
  (* set up local loop of a job worker *)
  try while true do
    let i, m = Utils.recv _router in
    let bar = m.bar in
    match m.typ with
    | Count -> (
      Logger.info "%s" ("count @ " ^ _addr);
      let y = List.length (Memory.find m.par.(0)) in
      Utils.send ~bar master OK [|Marshal.to_string y []|]
      )
    | Collect -> (
      Logger.info "%s" ("collect @ " ^ _addr);
      let y = Memory.find m.par.(0) in
      Utils.send ~bar master OK [|Marshal.to_string y []|]
      )
    | Broadcast -> (
      Logger.info "%s" ("broadcast @ " ^ _addr);
      Memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      Utils.send ~bar master OK [||]
      )
    | Reduce -> (
      Logger.info "%s" ("reduce @ " ^ _addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar master OK [|Marshal.to_string y []|];
      )
    | Fold -> (
      Logger.info "%s" ("fold @ " ^ _addr);
      let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar master OK [|Marshal.to_string y []|];
      )
    | Pipeline -> (
      Logger.info "%s" ("pipelined @ " ^ _addr);
      process_pipeline m.par;
      Utils.send ~bar master OK [||]
      )
    | Terminate -> (
      Logger.info "%s" ("terminate @ " ^ _addr);
      Utils.send ~bar master OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ _context.jid ^ " terminated")
      )
    | Load -> (
      Logger.info "%s" ("load " ^ m.par.(0) ^ " @ " ^ _addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let b = match (List.nth path 0) with
      | "unix"  -> Storage.unix_load (List.nth path 1)
      | _ -> Logger.info "%s" ("Error: unknown system!"); "" in
      Memory.add m.par.(1) [ b ];
      Utils.send ~bar master OK [||]
      )
    | Save -> (
      Logger.info "%s" ("save " ^ m.par.(0) ^ " @ " ^ _addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let c = match (List.nth path 0) with
      | "unix"  -> Storage.unix_save (List.nth path 1) (Memory.find m.par.(1))
      | _ -> Logger.info "%s" ("Error: unknown system!"); 0 in
      Utils.send ~bar master OK [|Marshal.to_string c []|]
      )
    | _ -> (
      Logger.debug "%s" ("Buffering " ^ _addr ^ " <- " ^ i ^ " m.bar : " ^ string_of_int (m.bar));
      Hashtbl.add _msgbuf m.bar (i,m)
      )
  done with Failure e -> (
    Logger.info "%s" e;
    ZMQ.Socket.(close master; close _router);
    Pervasives.exit 0 )

let init jid url =
  _context.jid <- jid;
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> master_fun m
    | Job_Worker -> worker_fun m
    | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req

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
  Logger.info "%s" ("terminate #" ^ _context.jid ^ "\n");
  let bar = _broadcast_all Terminate [||] in
  let _ = barrier bar in ()
  (* TODO: ZMQ.Context.terminate _ztx *)

let broadcast x =
  Logger.info "%s" ("broadcast -> " ^ string_of_int (StrMap.cardinal _context.worker) ^ " workers\n");
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
  let z = Marshal.to_string (StrMap.keys _context.worker) [] in
  let b = Marshal.to_string (Random.int 536870912) [] in
  Dag.add_edge (to_msg 0 ShuffleTask [|x; y; z; b|]) x y Blue; y

let reduce_by_key f x =
  (** TODO: without local combiner ... keep or not? *)
  let x = shuffle x in
  let y = Memory.rand_id () in
  Logger.info "%s" ("reduce_by_key " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 ReduceByKeyTask [|g; x; y|]) x y Red; y

let ___reduce_by_key f x =
  (** TODO: with local combiner ... keep or not? *)
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

let _ = Pervasives.at_exit (fun _ -> (** cleaning up *) ())
