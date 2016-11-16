(** [ Model Parallel ] Parameter server module  *)

open Types

let start jid url =
  let _ztx = ZMQ.Context.create () in
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  (* create and initialise part of the context *)
  let _context = Utils.empty_context () in
  _context.job_id <- jid;
  _context.myself_addr <- _addr;
  _context.myself_sock <- _router;
  _context.ztx <- _ztx;
  (* depends on the role, start server or client *)
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
  | Job_Master -> Paramserver.init m _context
  | Job_Worker -> Paramclient.init m _context
  | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req

(* scheduler funciton at master *)
let register_schedule (f : ('a, 'b, 'c) ps_schedule_typ) =
  Paramserver._schedule := Marshal.to_string f [ Marshal.Closures ]

(* aggregate function at master *)
let register_pull (f : ('a, 'b, 'c) ps_pull_typ) =
  Paramserver._pull := Marshal.to_string f [ Marshal.Closures ]

(* parallel execution at each worker *)
let register_push (f : ('a, 'b, 'c) ps_push_typ) =
  Paramclient._push := Marshal.to_string f [ Marshal.Closures ]

(* stopping criterion for the scheduling loop *)
let register_stop (f : unit -> bool) =
  Paramserver._stop := Marshal.to_string f [ Marshal.Closures ]

let get k =
  match Paramserver.(!_context.job_id) = "" with
  | true  -> Paramclient.get k
  | false -> Paramserver.get k

let set k v =
  match Paramserver.(!_context.job_id) = "" with
  | true  -> Paramclient.(set k v !_step)
  | false -> Paramserver.(set k v !_step)

let keys () = Hashtbl.fold (fun k v l -> l @ [ Obj.obj k ]) Paramserver._param []
