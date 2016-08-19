(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

module Actors = struct
  let _actors = ref StrMap.empty

  let create id addr = {
    id = id;
    addr = addr;
    last_seen = Unix.time ()
  }

  let add id addr = _actors := StrMap.add id (create id addr) !_actors
  let remove id = _actors := StrMap.remove id !_actors
  let mem id = StrMap.mem id !_actors
  let to_list () = StrMap.fold (fun k v l -> l @ [v]) !_actors []
  let addrs () = StrMap.fold (fun k v l -> l @ [v.addr]) !_actors []
end

let addr = "tcp://*:5555"
let myid = "manager_" ^ (string_of_int (Random.int 5000))

let process r m =
  match m.typ with
  | User_Reg -> (
    let uid, addr = m.par.(0), m.par.(1) in
    if Actors.mem uid = false then
      Utils.logger (uid ^ " @ " ^ addr);
      Actors.add uid addr;
      ZMQ.Socket.send r (Marshal.to_string OK []);
    )
  | Job_Reg -> (
    let master, jid = m.par.(0), m.par.(1) in
    if Service.mem jid = false then (
      Service.add jid master;
      let addrs = Marshal.to_string (Actors.addrs ()) [] in
      ZMQ.Socket.send r (to_msg Job_Master [|addrs; ""|]) )
    else
      let master = (Service.find jid).master in
      ZMQ.Socket.send r (to_msg Job_Worker [|master; ""|])
    )
  | Heartbeat -> (
    Utils.logger ("heartbeat @ " ^ m.par.(0));
    Actors.add m.par.(0) m.par.(1);
    ZMQ.Socket.send r (Marshal.to_string OK []);
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
