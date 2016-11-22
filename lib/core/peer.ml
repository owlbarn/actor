(** [ Peer-to-Peer Parallel ]  *)

open Types

let start jid url =
  let _ztx = ZMQ.Context.create () in
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req P2P_Reg [|_addr; jid|];
  (* create and initialise part of the context *)
  let _context = Utils.empty_peer_context () in
  _context.job_id <- jid;
  _context.myself_addr <- _addr;
  _context.myself_sock <- _router;
  _context.ztx <- _ztx;
  (* equivalent role, client is a new process *)
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
  | OK -> (
    match Unix.fork () with
    | 0 -> Peerclient.init m _context
    | p -> Peerserver.init m _context
    )
  | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req

(* basic architectural functions for p2p parallel *)

let register_barrier (f : p2p_barrier_typ) =
  Peerserver._barrier := Marshal.to_string f [ Marshal.Closures ]

let register_pull (f : ('a, 'b) p2p_pull_typ) =
  Peerserver._pull := Marshal.to_string f [ Marshal.Closures ]

let register_schedule (f : 'a p2p_schedule_typ) =
  Peerclient._schedule := Marshal.to_string f [ Marshal.Closures ]

let register_push (f : ('a, 'b) p2p_push_typ) =
  Peerclient._push := Marshal.to_string f [ Marshal.Closures ]

let register_stop (f : p2p_stop_typ) =
  Peerclient._stop := Marshal.to_string f [ Marshal.Closures ]

(* some helper functions for various strategies *)

let is_server () = Peerclient.(!_context.job_id) = ""

let get k =
  match is_server () with
  | true  -> Peerserver._get k
  | false -> Peerclient._get k

let set k v =
  match is_server () with
  | true  -> Peerserver.(_set k v !_context.step)
  | false -> Peerclient.(_set k v)

let swarm_size = None
