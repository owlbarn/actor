(** [ Parameter module ]
  model parallel model programming interface
*)

open Types

type ('a, 'b, 'c) ps_push_typ = 'a -> 'b list -> 'c list
type ('a, 'b) ps_schedule_typ = 'a list -> 'b list
type ('a, 'b) ps_pull_typ = 'a list -> 'b

let _ztx = ZMQ.Context.create ()

let init jid url =
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> Paramserver.init m jid _addr _router _ztx
    | Job_Worker -> Paramclient.init m jid _addr _router _ztx
    | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req

let get k = Paramclient.(get k !_step)

let set k v = Paramclient.(set k v !_step)

(** scheduler funciton at master *)
let register_schedule (f : ('a, 'b) ps_schedule_typ) =
  Paramserver._schedule := Marshal.to_string f [ Marshal.Closures ]

(** aggregate function at master *)
let register_pull (f : ('a, 'b) ps_pull_typ) =
  Paramserver._pull := Marshal.to_string f [ Marshal.Closures ]

(** parallel execution at each worker *)
let register_push (f : ('a, 'b, 'c) ps_push_typ) =
  Paramclient._push := Marshal.to_string f [ Marshal.Closures ]
