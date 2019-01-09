(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net  : Actor_net.Sig)
  (Sys  : Actor_sys.Sig)
  (Impl : Actor_param_impl.Sig)
  = struct

  include Actor_param_types.Make(Impl)


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


  let process context data =
    let m = decode_message data in
    let my_uuid = context.my_uuid in
    let my_addr = context.my_addr in

    match m.operation with
    | Reg_Rep -> (
        Owl_log.debug "<<< %s Reg_Rep" m.uuid;
        Lwt.return ()
      )
    | Exit -> (
        Owl_log.debug "<<< %s Exit" m.uuid;
        Lwt.return ()
      )
    | PS_Schd tasks -> (
        Owl_log.debug "<<< %s PS_Schd" m.uuid;
        let updates = Impl.push tasks in
        let s = encode_message my_uuid my_addr (PS_Push updates) in
        Net.send context.server_addr s
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init context =
    let%lwt () = Net.init () in

    (* register client to server *)
    let uuid = context.my_uuid in
    let addr = context.my_addr in
    let%lwt () = register context.server_addr uuid addr in

    (* start client service *)
    let thread_0 = heartbeat context.server_addr uuid addr in
    let thread_1 = Net.listen addr (process context) in
    let%lwt () = thread_0 in
    let%lwt () = thread_1 in

    (* clean up when client exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end
