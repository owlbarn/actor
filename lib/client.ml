(** []
  connect to Manager, represent a working node/actor.
*)

open Types

let manager_addr = "tcp://127.0.0.1:5555"

let my_addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 5000 + 5000))

let run id =
  (* register to the manager *)
  let context = ZMQ.Context.create () in
  let requester = ZMQ.Socket.create context ZMQ.Socket.req in
  ZMQ.Socket.connect requester manager_addr;
  ZMQ.Socket.send requester (to_msg User_Reg [|id; my_addr|]);
  ignore (ZMQ.Socket.recv requester);
  ZMQ.Socket.close requester;
  (* set up local service *)
  let rep = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  while true do
    let m = ZMQ.Socket.recv rep in
    print_endline m
  done;
  ZMQ.Socket.close rep;
  ZMQ.Context.terminate context


let () = run (Sys.argv.(1))
