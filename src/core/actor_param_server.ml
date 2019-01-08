(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_param_types


  let heartbeat context =
    let rec loop () =
      let%lwt () = Sys.sleep 10. in
      Owl_log.debug "Heartbeat %s" context.my_uuid;
      loop ()
    in
    loop ()


  let schedule uuid context =
    Owl_log.debug "Schedule %s" context.my_uuid;
    Actor_barrier_bsp.sync context.book uuid;
    let passed = Actor_barrier_bsp.pass context.book in
    let tasks = context.schedule passed in
    Array.iter (fun (uuid, task) ->
      Owl_log.debug ">>> %s Schedule ..." uuid;
      let addr = Actor_book.get_addr context.book uuid in
      let s = encode_message uuid addr (PS_Schd task) in
      Lwt.async (fun () -> Net.send addr s)
    ) tasks



  let process context data =
    let m = decode_message data in
    let my_uuid = context.my_uuid in
    let my_addr = context.my_addr in

    match m.operation with
    | Reg_Req -> (
        Owl_log.debug "<<< %s Reg_Req" m.uuid;
        Actor_book.set_addr context.book m.uuid m.addr;
        let s = encode_message my_uuid my_addr Reg_Rep in
        let%lwt () = Net.send m.addr s in
        if Actor_param_utils.is_ready context then
          schedule m.uuid context;
        Lwt.return ()
      )
    | Heartbeat -> (
        Owl_log.debug "<<< %s Heartbeat" m.uuid;
        Lwt.return ()
      )
    | PS_Get -> (
        Owl_log.debug "<<< %s PS_Get" m.uuid;
        Lwt.return ()
      )
    | PS_Set -> (
        Owl_log.debug "<<< %s PS_Set" m.uuid;
        Lwt.return ()
      )
    | PS_Push update -> (
        Owl_log.debug "<<< %s Push" m.uuid;
        Owl_log.warn "%s" update;
        schedule m.uuid context;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init context =
    let%lwt () = Net.init () in

    (* start server service *)
    let thread_0 = Net.listen context.my_addr (process context) in
    let thread_1 = heartbeat context in
    let%lwt () = thread_0 in
    let%lwt () = thread_1 in

    (* clean up when server exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end


(* ends here *)
