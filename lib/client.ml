(** []
  connect to Manager, represent a working node/actor.
*)

open Types

let manager = "tcp://127.0.0.1:5555"
let addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 5000 + 5000))
let myid = "actor_" ^ (string_of_int (Random.int 9000 + 1000))
let _ztx = ZMQ.Context.create ()

let register id u_addr m_addr =
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req m_addr;
  ZMQ.Socket.send req (to_msg User_Reg [|id; u_addr|]);
  ignore (ZMQ.Socket.recv req);
  ZMQ.Socket.close req

let heartbeat id m_addr =
  print_endline "send heartbeat ...";
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req m_addr;
  ZMQ.Socket.send req (to_msg Heartbeat [|id|]);
  ignore (ZMQ.Socket.recv req);
  print_endline "end heartbeat ...";
  ZMQ.Socket.close req

let run id addr =
  register myid addr manager;
  (* set up local service *)
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.set_receive_timeout rep 5000;
  ZMQ.Socket.bind rep addr;
  while true do
    try let m = of_msg (ZMQ.Socket.recv rep) in
      match m.typ with
      | _ -> ()
    with exn -> heartbeat myid addr
  done;
  ZMQ.Socket.close rep;
  ZMQ.Context.terminate _ztx


let () = run myid addr
