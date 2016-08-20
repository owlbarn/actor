(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  mutable jid : string;
  mutable master : string;
  mutable w_info : string list;
  mutable w_sock : [ `Dealer ] ZMQ.Socket.t list;
}

(* FIXME: global varibles ...*)
let _context = { jid = ""; master = ""; w_info = []; w_sock = []; }
let my_addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 5000 + 5000))
let _ztx = ZMQ.Context.create ()
let _router : [ `Router ] ZMQ.Socket.t = ZMQ.Socket.create _ztx ZMQ.Socket.router
let _ = ZMQ.Socket.bind _router my_addr

let recv s =
  let m = ZMQ.Socket.recv_all s in
  (List.nth m 0, List.nth m 1)

let send s i m = ZMQ.Socket.send_all s [i; m]

let _broadcast_all t s =
  List.iter (fun x -> ZMQ.Socket.send x (to_msg t s)) _context.w_sock

let _barrier l =
  let set = List.map (fun x -> x, ZMQ.Poll.In) l |> Array.of_list |> ZMQ.Poll.mask_of in
  let r = ref [] in
  try while (List.length !r) < (List.length l) do
    let poll_set = ZMQ.Poll.poll set |> Array.to_list in
    List.iter2 (fun x y ->
      match y with
      | Some ZMQ.Poll.In ->
        let z = ZMQ.Socket.recv x in
        r := !r @ [ Marshal.from_string z 0 ]
      | Some _ | None -> ()
    ) l poll_set
  done; !r with exn -> print_endline "error in barrier"; !r

let barrier l =
  let r = ref [] in
  while (List.length !r) < (List.length l) do
    let i, m = recv _router in
    r := !r @ [ Marshal.from_string m 0]
  done; !r

let process_pipeline s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
      Utils.logger ("map @ " ^ my_addr);
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      List.map f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FilterTask -> (
      Utils.logger ("filter @ " ^ my_addr);
      let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
      List.filter f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | UnionTask -> (
      Utils.logger ("union @ " ^ my_addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Memory.add m.par.(2)
      )
    | ShuffleTask -> (
      Utils.logger ("shuffle @ " ^ my_addr);
      (* TODO: tricky at the moment *)
      )
    | _ -> Utils.logger "unknow task types"
  ) s

let master_fun m =
  _context.master <- my_addr;
  (* contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  let l = List.map (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    ZMQ.Socket.send req (to_msg Job_Create [|my_addr; app; arg|]); req
  ) addrs in List.iter ZMQ.Socket.close l;
  (* wait until all the allocated actors register *)
  while (List.length _context.w_sock) < (List.length addrs) do
    let i, m = recv _router in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m;
    _context.w_info <- (m :: _context.w_info);
    _context.w_sock <- (s :: _context.w_sock);
  done

let worker_fun m =
  _context.master <- m.par.(0);
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.connect master _context.master;
  ZMQ.Socket.send master my_addr;
  (* TODO: connect to local actor *)
  (* set up job worker *)
  try while true do
    let i, m' = recv _router in
    let m = of_msg m' in
    match m.typ with
    | Count -> (
      Utils.logger ("count @ " ^ my_addr);
      let y = Marshal.to_string (List.length (Memory.find m.par.(0))) [] in
      ZMQ.Socket.send master y
      )
    | Collect -> (
      Utils.logger ("collect @ " ^ my_addr);
      let y = Marshal.to_string (Memory.find m.par.(0)) [] in
      ZMQ.Socket.send master y
      )
    | Broadcast -> (
      Utils.logger ("broadcast @ " ^ my_addr);
      Memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      ZMQ.Socket.send master (Marshal.to_string OK [])
      )
    | Fold -> (
      Utils.logger ("fold @ " ^ my_addr);
      let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in ZMQ.Socket.send master (Marshal.to_string y []);
      )
    | Pipeline -> (
      Utils.logger ("pipelined @ " ^ my_addr);
      process_pipeline m.par;
      ZMQ.Socket.send master (Marshal.to_string OK [])
      )
    | Terminate -> (
      Utils.logger ("terminate @ " ^ my_addr);
      ZMQ.Socket.send master (Marshal.to_string OK []);
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith "terminated"
      )
    | _ -> ()
  done with exn -> (
    Utils.logger "task finished.";
    (*List.iter ZMQ.Socket.close [router; dealer];*)
    Pervasives.exit 0
  )

let init jid url =
  _context.jid <- jid;
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  ZMQ.Socket.send req (to_msg Job_Reg [|my_addr; jid|]);
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> master_fun m
    | Job_Worker -> worker_fun m
    | _ -> Utils.logger "unknown command";
  ZMQ.Socket.close req

let run_job () =
  List.iter (fun s ->
    let s' = List.map (fun x -> Dag.get_vlabel_f x) s in
    _broadcast_all Pipeline (Array.of_list s');
    barrier _context.w_sock;
    Dag.mark_stage_done s;
  ) (Dag.stages ())

let collect x =
  Utils.logger ("collect " ^ x ^ "\n");
  run_job ();
  _broadcast_all Collect [|x|];
  barrier _context.w_sock

let count x =
  Utils.logger ("count " ^ x ^ "\n");
  run_job ();
  _broadcast_all Count [|x|];
  barrier _context.w_sock |> List.fold_left (+) 0

let fold f a x =
  Utils.logger ("fold " ^ x ^ "\n");
  run_job ();
  let g = Marshal.to_string f [ Marshal.Closures ] in
  _broadcast_all Fold [|g; x|];
  barrier _context.w_sock
  |> List.filter (function Some x -> true | None -> false)
  |> List.map (function Some x -> x | None -> failwith "")
  |> List.fold_left f a

let terminate () =
  Utils.logger ("terminate job " ^ _context.jid ^ "\n");
  _broadcast_all Terminate [||];
  barrier _context.w_sock

let broadcast x =
  Utils.logger ("broadcast -> " ^ string_of_int (List.length _context.w_sock) ^ " workers\n");
  let y = Memory.rand_id () in
  _broadcast_all Broadcast [|Marshal.to_string x []; y|];
  barrier _context.w_sock; y

let get_value x = Memory.find x

let map f x =
  let y = Memory.rand_id () in
  Utils.logger ("map " ^ x ^ " -> " ^ y ^ "\n");
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg MapTask [|g; x; y|]) x y Red; y

let filter f x =
  Utils.logger ("filter " ^ x ^ "\n");
  let y = Memory.rand_id () in
  let g = Marshal.to_string f [ Marshal.Closures ] in
  Dag.add_edge (to_msg FilterTask [|g; x; y|]) x y Red; y

let union x y =
  Utils.logger ("union " ^ x ^ " and " ^ y ^ "\n");
  let z = Memory.rand_id () in
  Dag.add_edge (to_msg UnionTask [|x; y; z|]) x z Red;
  Dag.add_edge (to_msg UnionTask [|x; y; z|]) y z Red; z

let shuffle x =
  Utils.logger ("shuffle " ^ x ^ "\n");
  let y = Memory.rand_id () in
  let z = Marshal.to_string _context.w_info [] in
  Dag.add_edge (to_msg ShuffleTask [|x; y; z|]) x y Blue; y
