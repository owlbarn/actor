(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Actor: connect to Manager, represent a working node/actor. *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  module Types = Actor_types.Make(Net)
  open Types


  let register sock id w_addr m_addr =
    Owl_log.info "%s" ("register -> " ^ m_addr);
    let s = Marshal.to_string (User_Reg (id, w_addr)) [] in
    Net.send sock s


  let heartbeat sock id w_addr m_addr =
    Owl_log.info "%s" ("heartbeat -> " ^ m_addr);
    let s = Marshal.to_string (Heartbeat (id, w_addr)) [] in
    Net.send sock s


  let start_app app args =
    Owl_log.info "%s" ("starting job " ^ app);
    Sys.exec app args;
    Lwt.return ()


  let deploy_app _x =
    Owl_log.error "cannot find %s" _x;
    Lwt.return ()


  let run id w_addr m_addr =
    (* set up connection to manager *)
    let m_sock = Net.socket () in
    let%lwt () = Net.connect m_sock m_addr in
    let%lwt () = register m_sock id w_addr m_addr in

    (* set up local service *)
    let w_sock = Net.listen w_addr in

    (* define service loop *)
    let rec loop continue =
      if continue then (
        let%lwt pkt = Net.recv w_sock in
        let msg = of_msg pkt in
        let%lwt () =
          match msg with
          | Job_Create (uid, app, arg) -> (
              Owl_log.info "%s" (app ^ " <- " ^ uid);
              let s = Marshal.to_string OK [] in
              let%lwt () = Net.send w_sock s in
              match%lwt Sys.file_exists app with
              | true  -> start_app app arg
              | false -> deploy_app app
            )
          | _ -> Lwt.return ()
        in
        loop true
      )
      else (
        let%lwt () = Net.close w_sock in
        let%lwt () = Net.close m_sock in
        let%lwt () = Net.exit () in
        Lwt.return ()
      )
    in

    (* define periodic tasks *)
    let rec timer () =
      let%lwt () = Sys.sleep 10. in
      let%lwt () = heartbeat m_sock id w_addr m_addr in
      timer ()
    in

    (* start worker service *)
    let%lwt () = timer () in
    let%lwt () = loop true in
    Lwt.return ()


end
