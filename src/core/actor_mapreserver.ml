(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_types


  let process pkt =
    let msg = of_msg pkt in
    match msg with
    | Reg_Req (uuid, addr) -> (
        Owl_log.debug "Reg_Req (%s, %s)" uuid addr;
        Lwt.return ()
      )
    | Heartbeat (uuid, addr) -> (
        Owl_log.debug "Heartbeat (%s, %s)" uuid addr;
        Lwt.return ()
      )
    | _ -> (
        Owl_log.error "unknown message type";
        Lwt.return ()
      )


  let init config =
    let%lwt () = Net.init () in

    (* start server service *)
    let uuid = config.myself in
    let addr = Hashtbl.find config.book uuid in
    let%lwt () = Net.listen addr process in

    (* clean up when server exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end


(* ends here *)
