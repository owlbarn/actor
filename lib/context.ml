(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  mutable jid : string;
  mutable master : string;
  mutable worker : [`Dealer] ZMQ.Socket.t StrMap.t;
}

(* FIXME: global varibles ...*)
let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _ztx = ZMQ.Context.create ()
let _addr, _router = Utils.bind_available_addr _ztx

let _broadcast_all t s =
  let bar = Random.int 536870912 in
  StrMap.iter (fun k v -> Utils.send ~bar v t s) _context.worker;
  bar

let bsp_barrier b x =
  let h = Hashtbl.create 1024 in
  while (Hashtbl.length h) < (StrMap.cardinal x) do
    let i, m = Utils.recv _router in
    if b = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m
  done;
  Hashtbl.fold (fun k v l -> v :: l) h []

let process_pipeline s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
      Utils.logger ("map @ " ^ _addr);
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      List.map f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FilterTask -> (
      Utils.logger ("filter @ " ^ _addr);
      let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
      List.filter f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FlattenTask -> (
      Utils.logger ("flatten @ " ^ _addr);
      List.flatten (Memory.find m.par.(0)) |> Memory.add m.par.(1)
      )
    | UnionTask -> (
      Utils.logger ("union @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Memory.add m.par.(2)
      )
    | ReduceTask -> (
      Utils.logger ("reduce @ " ^ _addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      Memory.find m.par.(1) |> Utils.group_by_key |> List.map (fun (k,l) ->
        match l with hd :: tl -> (k, List.fold_left f hd tl)
        | [] -> failwith "error in reduce"
      ) |> Memory.add m.par.(2)
      )
    | JoinTask -> (
      Utils.logger ("join @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Utils.group_by_key |> Memory.add m.par.(2)
      )
    | ShuffleTask -> (
      Utils.logger ("shuffle @ " ^ _addr);
      let x = Memory.find m.par.(0) |> Utils.group_by_key in
      let z = Marshal.from_string m.par.(2) 0 in
      let l = List.mapi (fun i k ->
        let v = Utils.choose_load x (List.length z) i in
        let s = ZMQ.Socket.(create _ztx dealer) in
        ZMQ.Socket.(set_identity s _addr; connect s k);
        Utils.send s OK [|Marshal.to_string v []|]; s) z in
      List.fold_left (fun x _ -> (Utils.recv _router |> snd) :: x) [] l
      |> List.map (fun m -> Marshal.from_string m.par.(0) 0 |> Utils.flatten_kvg)
      |> List.flatten |> Memory.add m.par.(1);
      List.iter ZMQ.Socket.close l;
      )
    | _ -> Utils.logger "unknow task types"
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
    | OK -> (
      print_endline ("OK <- " ^ i ^ " m.bar : " ^ string_of_int (m.bar));
      )
    | Count -> (
      Utils.logger ("count @ " ^ _addr);
      let y = List.length (Memory.find m.par.(0)) in
      Utils.send ~bar master OK [|Marshal.to_string y []|]
      )
    | Collect -> (
      Utils.logger ("collect @ " ^ _addr);
      let y = Memory.find m.par.(0) in
      Utils.send ~bar master OK [|Marshal.to_string y []|]
      )
    | Broadcast -> (
      Utils.logger ("broadcast @ " ^ _addr);
      Memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      Utils.send ~bar master OK [||]
      )
    | Fold -> (
      Utils.logger ("fold @ " ^ _addr);
      let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar master OK [|Marshal.to_string y []|];
      )
    | Pipeline -> (
      Utils.logger ("pipelined @ " ^ _addr);
      process_pipeline m.par;
      Utils.send ~bar master OK [||]
      )
    | Terminate -> (
      Utils.logger ("terminate @ " ^ _addr);
      Utils.send ~bar master OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ _context.jid ^ " terminated")
      )
    | _ -> ()
  done with Failure e -> (
    Utils.logger e;
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
    | _ -> Utils.logger "unknown command";
  ZMQ.Socket.close req

let run_job_eager () =
  List.iter (fun s ->
    let s' = List.map (fun x -> Dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = bsp_barrier bar _context.worker in
    Dag.mark_stage_done s;
  ) (Dag.stages_eager ())

let run_job_lazy x =
  List.iter (fun s ->
    let s' = List.map (fun x -> Dag.get_vlabel_f x) s in
    let bar = _broadcast_all Pipeline (Array.of_list s') in
    let _ = bsp_barrier bar _context.worker in
    Dag.mark_stage_done s;
  ) (Dag.stages_lazy x)

let collect x =
  Utils.logger ("collect " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Collect [|x|] in
  bsp_barrier bar _context.worker
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)

let count x =
  Utils.logger ("count " ^ x ^ "\n");
  run_job_lazy x;
  let bar = _broadcast_all Count [|x|] in
  bsp_barrier bar _context.worker
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.fold_left (+) 0

let fold f a x =
  Utils.logger ("fold " ^ x ^ "\n");
  run_job_lazy x;
  let g = Marshal.to_string f [ Marshal.Closures ] in
  let bar = _broadcast_all Fold [|g; x|] in
  bsp_barrier bar _context.worker
  |> List.map (fun m -> Marshal.from_string m.par.(0) 0)
  |> List.filter (function Some x -> true | None -> false)
  |> List.map (function Some x -> x | None -> failwith "")
  |> List.fold_left f a

let terminate () =
  Utils.logger ("terminate #" ^ _context.jid ^ "\n");
  let bar = _broadcast_all Terminate [||] in
  let _ = bsp_barrier bar _context.worker in ()

let broadcast x =
  Utils.logger ("broadcast -> " ^ string_of_int (StrMap.cardinal _context.worker) ^ " workers\n");
  let y = Memory.rand_id () in
  let bar = _broadcast_all Broadcast [|Marshal.to_string x []; y|] in
  let _ = bsp_barrier bar _context.worker in y

let get_value x = Memory.find x

let map f x =
  let y = Memory.rand_id () in
  Utils.logger ("map " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 MapTask [|g; x; y|]) x y Red; y

let filter f x =
  let y = Memory.rand_id () in
  Utils.logger ("filter " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 FilterTask [|g; x; y|]) x y Red; y

let flatten x =
  let y = Memory.rand_id () in
  Utils.logger ("flatten " ^ x ^ " -> " ^ y ^ "\n");
  Dag.add_edge (to_msg 0 FlattenTask [|x; y|]) x y Red; y

let union x y =
  let z = Memory.rand_id () in
  Utils.logger ("union " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  Dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) x z Red;
  Dag.add_edge (to_msg 0 UnionTask [|x; y; z|]) y z Red; z

let shuffle x =
  let y = Memory.rand_id () in
  Utils.logger ("shuffle " ^ x ^ " -> " ^ y ^ "\n");
  let z = Marshal.to_string (StrMap.keys _context.worker) [] in
  Dag.add_edge (to_msg 0 ShuffleTask [|x; y; z|]) x y Blue; y

let reduce f x =
  let y = Memory.rand_id () in
  Utils.logger ("reduce " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg 0 ReduceTask [|g; x; y|]) x y Red; y

let join x y =
  let z = Memory.rand_id () in
  Utils.logger ("join " ^ x ^ " & " ^ y ^ " -> " ^ z ^ "\n");
  let x, y = shuffle x, shuffle y in
  Dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) x z Red;
  Dag.add_edge (to_msg 0 JoinTask [|x; y; z|]) y z Red; z
