(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_param_types


  let register s_addr c_uuid c_addr =
    Owl_log.debug ">>> %s Reg_Req" s_addr;
    let s = encode_message c_uuid c_addr Reg_Req in
    Net.send s_addr s


  let heartbeat s_addr c_uuid c_addr =
    let rec loop () =
      let%lwt () = Sys.sleep 10. in
      Owl_log.debug ">>> %s Heartbeat" s_addr;
      let s = encode_message c_uuid c_addr Heartbeat in
      let%lwt () = Net.send s_addr s in
      loop ()
    in
    loop ()


  let process data =
    let m = decode_message data in
    match m.operation with
    | Reg_Rep -> (
        Owl_log.debug "<<< %s Reg_Rep" m.uuid;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init context =
    let%lwt () = Net.init () in

    (* register client to server *)
    let s_uuid = context.myself in
    let s_addr = Hashtbl.find context.book s_uuid in
    let m_uuid = context.server in
    let m_addr = Hashtbl.find context.book m_uuid in
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
