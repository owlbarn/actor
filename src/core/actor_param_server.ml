(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_param_types


  let dummy_barrier context =
    Actor_param_utils.htbl_to_arr context.book
    |> Array.map (fun (_, v) -> Actor_book.(v.uuid))


  let heartbeat context =
    let rec loop () =
      let%lwt () = Sys.sleep 10. in
      Owl_log.debug "Heartbeat %s" context.my_uuid;
      loop ()
    in
    loop ()


  let schedule context =
    Owl_log.debug "Schedule %s" context.my_uuid;
    let passed = dummy_barrier context in
    Array.iter (fun c ->
      Owl_log.debug "%s passed ..." c;
    ) passed


  let process context data =
    let m = decode_message data in
    match m.operation with
    | Reg_Req -> (
        Owl_log.debug "<<< %s Reg_Req" m.uuid;
        Actor_book.set_addr context.book m.uuid m.addr;
        let uuid = context.my_uuid in
        let addr = context.my_addr in
        let s = encode_message uuid addr Reg_Rep in
        let%lwt () = Net.send m.addr s in
        if Actor_param_utils.is_ready context then
          schedule context;
        Lwt.return ()
      )
    | Heartbeat -> (
        Owl_log.debug "<<< %s Heartbeat" m.uuid;
        Lwt.return ()
      )
    | Exit -> (
        Owl_log.debug "<<< %s Exit" m.uuid;
        Lwt.return ()
      )
    | PS_Push -> (
        Owl_log.debug "<<< %s Push" m.uuid;
        schedule context;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let iterate context =
    let rec loop i =
      let%lwt () = Sys.sleep 10. in
      Owl_log.debug "%s iter #%i" context.my_uuid i;

      (* let next () = loop (i + 1) in
      Actor_barrier_bsp.wait i next *)
      loop(i + 1)
    in
    loop 0


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
