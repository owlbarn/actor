(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Actor: connect to Manager, represent a working node/actor. *)

module Make
  (Net : Actor_net.Sig)
  = struct

  module Types = Actor_types.Make(Net)
  open Types

let manager = Actor_config.manager_addr
let addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000))
let myid = "worker_" ^ (string_of_int (Random.int 9000 + 1000))

let register req id u_addr m_addr =
  Owl_log.info "%s" ("register -> " ^ m_addr);
  let s = Marshal.to_string (User_Reg (id, u_addr)) [] in
  Net.send req s
  (* ignore (Zmq.Socket.recv req) *)

let heartbeat req id u_addr m_addr =
  Owl_log.info "%s" ("heartbeat -> " ^ m_addr);
  let s = Marshal.to_string (Heartbeat (id, u_addr)) [] in
  Net.send req s
  (* ignore (Zmq.Socket.recv req) *)

let start_app app arg =
  Owl_log.info "%s" ("starting job " ^ app);
  match Unix.fork () with
  | 0 -> if Unix.fork () = 0 then Unix.execv app arg else exit 0
  | _p -> ignore(Unix.wait ())

let deploy_app _x = Owl_log.error "cannot find %s" _x

let run id u_addr m_addr =
  (* set up connection to manager *)
  let req = Net.socket () in
  Net.connect req m_addr;
  register req myid u_addr m_addr;
  (* set up local service *)
  let rep = Net.listen u_addr in
  while true do
    Net.timeout rep (3 * 1000);
    try
      let msg = of_msg (Net.recv rep) in
      match msg with
      | Job_Create (uid, app, arg) -> (
        Owl_log.info "%s" (app ^ " <- " ^ uid);
        Net.send rep (Marshal.to_string OK []);
        match Sys.file_exists app with
        | true -> start_app app arg
        | false -> deploy_app app
        )
      | _ -> ()
    with
      | Unix.Unix_error (_,_,_) -> heartbeat req id u_addr m_addr
      | Zmq.ZMQ_exception (_,s) -> Owl_log.error "%s" s
      | _exn -> Owl_log.error "unknown error"
  done;
  Net.close rep;
  Net.close req;
  Net.exit ()


end
