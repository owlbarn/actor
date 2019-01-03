(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Actor: connect to Manager, represent a working node/actor. *)

open Actor_types

let manager = Actor_config.manager_addr
let addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000))
let myid = "worker_" ^ (string_of_int (Random.int 9000 + 1000))
let _ztx = Zmq.Context.create ()

let register req id u_addr m_addr =
  Owl_log.info "%s" ("register -> " ^ m_addr);
  Actor_utils.send req User_Reg [|id; u_addr|];
  ignore (Zmq.Socket.recv req)

let heartbeat req id u_addr m_addr =
  Owl_log.info "%s" ("heartbeat -> " ^ m_addr);
  Actor_utils.send req Heartbeat [|id; u_addr|];
  ignore (Zmq.Socket.recv req)

let start_app app arg =
  Owl_log.info "%s" ("starting job " ^ app);
  match Unix.fork () with
  | 0 -> if Unix.fork () = 0 then Unix.execv app arg else exit 0
  | _p -> ignore(Unix.wait ())

let deploy_app _x = Owl_log.error "cannot find %s" _x

let run id u_addr m_addr =
  (* set up connection to manager *)
  let req = Zmq.Socket.create _ztx Zmq.Socket.req in
  Zmq.Socket.connect req m_addr;
  register req myid u_addr m_addr;
  (* set up local service *)
  let rep = Zmq.Socket.create _ztx Zmq.Socket.rep in
  Zmq.Socket.bind rep u_addr;
  while true do
    Zmq.Socket.set_receive_timeout rep (300 * 1000);
    try let m = of_msg (Zmq.Socket.recv rep) in
      match m.typ with
      | Job_Create -> (
        let app = m.par.(1) in
        let arg = Marshal.from_string m.par.(2) 0 in
        Owl_log.info "%s" (app ^ " <- " ^ m.par.(0));
        Zmq.Socket.send rep (Marshal.to_string OK []);
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
  Zmq.Socket.close rep;
  Zmq.Socket.close req;
  Zmq.Context.terminate _ztx

let () = run myid addr manager
