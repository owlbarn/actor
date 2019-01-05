(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Actor: connect to Manager, represent a working node/actor. *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
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
  let _ =
    match Unix.fork () with
    | 0 -> if Unix.fork () = 0 then Unix.execv app arg else exit 0
    | _p -> ignore(Unix.wait ())
  in
  Lwt.return ()

let deploy_app _x =
  Owl_log.error "cannot find %s" _x;
  Lwt.return ()

let run id u_addr m_addr =
  (* set up connection to manager *)
  let req = Net.socket () in
  let%lwt () = Net.connect req m_addr in
  let%lwt () = register req myid u_addr m_addr in
  (* set up local service *)
  let rep = Net.listen u_addr in

  let rec loop continue =
    if continue then (
      let%lwt pkt = Net.recv rep in
      let msg = of_msg pkt in
      let%lwt () =
        match msg with
        | Job_Create (uid, app, arg) -> (
          Owl_log.info "%s" (app ^ " <- " ^ uid);
          let%lwt () = Net.send rep (Marshal.to_string OK []) in
          let%lwt () =
            match%lwt Sys.file_exists app with
            | true  -> start_app app arg
            | false -> deploy_app app
          in
          Lwt.return ()
          )
        | _ -> Lwt.return ()
      in
      loop true
    )
    else (
      let%lwt () = Net.close rep in
      let%lwt () = Net.close req in
      Net.exit ()
    )

  in

  let rec timer () =
    let%lwt () = Lwt_unix.sleep 10. in
    let%lwt () = heartbeat req id u_addr m_addr in
    timer ()
  in

  let%lwt () = timer () in
  let%lwt () = loop true in
  Lwt.return ()



end
