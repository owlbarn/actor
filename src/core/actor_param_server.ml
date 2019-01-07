(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_param_types


  let process context data =
    let m = decode_message data in
    match m.operation with
    | Reg_Req -> (
        Owl_log.debug "<<< %s Reg_Req" m.uuid;
        Hashtbl.replace context.book m.uuid m.addr;
        let uuid = context.myself in
        let addr = Hashtbl.find context.book uuid in
        let s = encode_message uuid addr Reg_Rep in
        Actor_param_utils.is_ready context;
        Net.send m.addr s
      )
    | Heartbeat -> (
        Owl_log.debug "<<< %s Heartbeat" m.uuid;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init context =
    let%lwt () = Net.init () in

    (* start server service *)
    let uuid = context.myself in
    let addr = Hashtbl.find context.book uuid in
    let%lwt () = Net.listen addr (process context) in

    (* clean up when server exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end


(* ends here *)
