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
      Utils.logger (uid ^ " @ " ^ addr);
      ZMQ.Socket.send r "ok"
    )
  | Job_Reg -> (
    let master, jid = m.par.(0), m.par.(1) in
    if Service.mem jid = false then (
      Service.add jid master;
      let addrs = Marshal.to_string (Actor.addrs ()) [] in
      ZMQ.Socket.send r (to_msg Job_Master [|addrs; ""|]) )
    else
      let master = (Service.find jid).master in
      ZMQ.Socket.send r (to_msg Job_Worker [|master; ""|])
    )
  | Heartbeat -> (
    Utils.logger ("heartbeat @ " ^ m.par.(0));
    (* TODO: update actor information *)
    ZMQ.Socket.send r "ok"
    )
  | Data_Reg -> ()
  | _ -> ()

let run id addr =
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

let _ = run myid addr
