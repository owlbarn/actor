(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_mapre_types


  let process contex data =
    let m = decode_message data in
    match m.operation with
    | Reg_Req -> (
        Owl_log.debug "<<< %s Reg_Req" m.uuid;
        Hashtbl.replace contex.book m.uuid m.addr;
        let uuid = contex.myself in
        let addr = Hashtbl.find contex.book uuid in
        let s = encode_message uuid addr Reg_Rep in
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


  let init contex =
    let%lwt () = Net.init () in

    (* start server service *)
    let uuid = contex.myself in
    let addr = Hashtbl.find contex.book uuid in
    let%lwt () = Net.listen addr (process contex) in

    (* clean up when server exits *)
    let%lwt () = Net.exit () in
    Lwt.return ()


end


(* ends here *)
