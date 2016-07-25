(** []
  connect to Manager, represent a working node/actor.
*)

let manager_addr = "tcp://localhost:5555"

let run id =
  let context = ZMQ.Context.create () in
  let requester = ZMQ.Socket.create context ZMQ.Socket.req in
  ZMQ.Socket.connect requester manager_addr;
  ZMQ.Socket.send requester id;
  while true do
    Unix.sleep 5;
    print_endline "heart beat ..."
  done;
  ZMQ.Socket.close requester;
  ZMQ.Context.terminate context

let () = run (Sys.argv.(1))
