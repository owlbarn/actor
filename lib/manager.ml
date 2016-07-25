(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

let addr = "tcp://*:5555"

let actors = ref StrMap.empty

let run () =
  let context = ZMQ.Context.create () in
  let responder = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind responder addr;
  while true do
    let s = ZMQ.Socket.recv responder in
    Printf.printf "[register]: %s\n%!" s;
    if StrMap.mem s !actors then
      actors := StrMap.add s (Actor.create s) !actors;
    ZMQ.Socket.send responder "confirmed"
  done;
  ZMQ.Socket.close responder;
  ZMQ.Context.terminate context

let install_app x = None

let _ = run ()
