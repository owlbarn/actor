(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

let addr = "tcp://*:5555"

let run () =
  let context = ZMQ.Context.create () in
  let responder = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind responder addr;
  while true do
    let s = ZMQ.Socket.recv responder in
    Printf.printf "[register]: %s\n%!" s;
    if Actor.mem s = false then (
      Actor.add s;
      ZMQ.Socket.send responder "master" )
    else
      ZMQ.Socket.send responder "worker"
  done;
  ZMQ.Socket.close responder;
  ZMQ.Context.terminate context

let install_app x = None

let _ = run ()
