(** [ client ]
  registers to the server
*)

let context = ZMQ.Context.create () in
print_endline "Connecting to hello world server...";
let requester = ZMQ.Socket.create context ZMQ.Socket.req in
ZMQ.Socket.connect requester "tcp://localhost:5555";

for i = 1 to 5 do
  Printf.printf "Sending request %d...\n" i;
  ZMQ.Socket.send requester "Hello";
  let reply = ZMQ.Socket.recv requester in
  Printf.printf "call the serialized function %i ... \n" i;
  let f = Actor.from_string reply in
  f ()
done;

ZMQ.Socket.close requester;
ZMQ.Context.terminate context
