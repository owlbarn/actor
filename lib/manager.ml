(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

let addr = "tcp://*:5555"

let process r m =
  match m.typ with
  | User_Reg -> (
    if Actor.mem m.str = false then
      Actor.add m.str;
      Printf.printf "[manager]: user_reg %s\n%!" m.str;
      ZMQ.Socket.send r "ok"
    )
  | Job_Reg -> (
    if Service.mem m.str = false then (
      Service.add m.str ""; (* FIXME: node id *)
      ZMQ.Socket.send r "master" )
    else
      ZMQ.Socket.send r "worker"
    )
  | Heartbeat -> (
    print_endline "[manager]: heartbeat from client";
    ZMQ.Socket.send r "ok"
    )
  | Data_Reg -> ()

let run id =
  let context = ZMQ.Context.create () in
  let responder = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind responder addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv responder) in
    process responder m;
  done;
  ZMQ.Socket.close responder;
  ZMQ.Context.terminate context

let install_app x = None

let _ = run (Sys.argv.(1))
