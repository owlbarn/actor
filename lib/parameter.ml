(** [ Parameter module ]
  model parallel model programming interface
*)

open Types

let _ztx = ZMQ.Context.create ()

let init jid url =
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> Paramserver.master_init m jid _addr _router _ztx
    | Job_Worker -> Paramclient.worker_init m jid _addr _router _ztx
    | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req

let get k = Paramclient.(get k !_step)

let set k v = Paramclient.(set k v !_step)

let register_schedule f = None
(** scheduler funciton at master *)

let register_pull f = None
(** aggregate function at master *)

let register_push f = None
(** parallel execution at each worker *)

let terminate () = Paramclient.terminate ()
