(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_types


  let register m_addr s_uuid s_addr =
    Owl_log.debug "Reg_Req (%s, %s)" s_uuid s_addr;
    let s = Marshal.to_string (Reg_Req (s_uuid, s_addr)) [] in
    Net.send m_addr s


  let heartbeat m_addr s_uuid s_addr =
    let rec loop () =
      let%lwt () = Sys.sleep 10. in
      Owl_log.debug "Heartbeat (%s, %s)" s_uuid s_addr;
      let s = Marshal.to_string (Heartbeat (s_uuid, s_addr)) [] in
      let%lwt () = Net.send m_addr s in
      loop ()
    in
    loop ()


  let process pkt =
    let msg = of_msg pkt in
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

    (* register client to server *)
    let s_uuid = config.myself in
    let s_addr = Hashtbl.find config.book s_uuid in
    let m_uuid = config.server in
    let m_addr = Hashtbl.find config.book m_uuid in
    let%lwt () = register m_addr s_uuid s_addr in

    (* start client service *)
    let thread_0 = heartbeat m_addr s_uuid s_addr in
    let thread_1 = Net.listen s_addr process in
    let%lwt () = thread_0 in
    let%lwt () = thread_1 in

    (* clean up when client exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end


(* ends here *)
