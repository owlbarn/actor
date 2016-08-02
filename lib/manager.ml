(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

let addr = "tcp://*:5555"
let myid = "manager_" ^ (string_of_int (Random.int 5000))

let process r m =
  match m.typ with
  | User_Reg -> (
    let uid, addr = m.par.(0), m.par.(1) in
    if Actor.mem uid = false then
      Actor.add uid addr;
      Printf.printf "[manager]: %s @ %s\n%!" uid addr;
      ZMQ.Socket.send r "ok"
    )
  | Job_Reg -> (
    let master, jid = m.par.(0), m.par.(1) in
    if Service.mem jid = false then (
      Service.add jid master;
      ZMQ.Socket.send r (to_msg Job_Master [|""; ""|]) )
    else
      let master = (Service.find jid).master in
      ZMQ.Socket.send r (to_msg Job_Worker [|master; ""|])
    )
  | Heartbeat -> (
    print_endline "[manager]: heartbeat from client";
    ZMQ.Socket.send r "ok"
    )
  | Data_Reg -> ()
  | _ -> ()

let run id =
  let _ztx = ZMQ.Context.create () in
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv rep) in
    process rep m;
  done;
  ZMQ.Socket.close rep;
  ZMQ.Context.terminate _ztx

let install_app x = None

let _ = run myid
