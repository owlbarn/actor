(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  module Types = Actor_types.Make (Net)

  open Types


  let register sock s_uuid s_addr =
    Owl_log.debug "Reg_Req (%s, %s)" s_uuid s_addr;
    let s = Marshal.to_string (Reg_Req (s_uuid, s_addr)) [] in
    Net.send sock s


  let heartbeat sock s_uuid s_addr =
    Owl_log.debug "Heartbeat (%s, %s)" s_uuid s_addr;
    let s = Marshal.to_string (Heartbeat (s_uuid, s_addr)) [] in
    Net.send sock s


  let process _sock msg =
    match msg with
    | Reg_Rep result -> (
        Owl_log.debug "Reg_Rep %s" result;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init config =
    let%lwt () = Net.init () in

    (* set up server of local slave node *)
    let s_uuid = config.myself in
    let s_addr = StrMap.find s_uuid config.uri in
    let s_sock = Net.listen s_addr in

    (* set up connection to remote master *)
    let m_sock = Net.socket () in
    let m_uuid = config.server in
    let m_addr = StrMap.find m_uuid config.uri in
    let%lwt () = Net.connect m_sock m_addr in
    let%lwt () = register m_sock s_uuid s_addr in

    (* define service loop *)
    let rec loop continue =
      if continue then (
        let%lwt pkt = Net.recv s_sock in
        let msg = of_msg pkt in
        let%lwt () = process s_sock msg in
        loop true
      )
      else (
        let%lwt () = Net.close m_sock in
        let%lwt () = Net.close s_sock in
        let%lwt () = Net.exit () in
        Lwt.return ()
      )
    in

    (* define periodic tasks *)
    let rec timer () =
      let%lwt () = Sys.sleep 10. in
      let%lwt () = heartbeat m_sock s_uuid s_addr in
      timer ()
    in

    (* start worker service *)
    let%lwt () = timer () in
    let%lwt () = loop true in
    Lwt.return ()


end


(* ends here *)
