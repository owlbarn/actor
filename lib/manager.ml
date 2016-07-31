(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

let addr = "tcp://*:5555"

let process r m =
  match m.typ with
  | User_Reg -> (
    if Actor.mem m.str = false then
      Actor.add m.str
    )
  | Job_Reg -> (
    if Actor.mem m.str = false then (
      Actor.add m.str;
      ZMQ.Socket.send r "master" )
    else
      ZMQ.Socket.send r "worker"
    )

let run () =
  let context = ZMQ.Context.create () in
  let responder = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind responder addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv responder) in
    Printf.printf "[manager]: %s\n%!" m.str;
    process responder m
  done;
  ZMQ.Socket.close responder;
  ZMQ.Context.terminate context

let install_app x = None

let _ = run ()
