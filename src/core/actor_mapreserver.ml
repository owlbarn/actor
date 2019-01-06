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


  let process config sock msg =
    match msg with
    | Reg_Req (uuid, addr) -> (
        Owl_log.debug "Reg_Req (%s, %s)" uuid addr;
        if Hashtbl.mem config.waiting uuid then (
          Hashtbl.remove config.waiting uuid;
          if Hashtbl.length config.waiting = 0 then (
            Owl_log.debug "start app ...";
          )
        );
        let s = Marshal.to_string (Reg_Rep "OK") [] in
        Net.send sock s
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
    let uuid = config.myself in
    let addr = StrMap.find uuid config.uri in
    let sock = Net.listen addr in

    let rec loop continue =
      if continue then (
        let%lwt pkt = Net.recv sock in
        let msg = of_msg pkt in
        let%lwt () = process config sock msg in
        loop true
      )
      else (
        let%lwt () = Net.close sock in
        let%lwt () = Net.exit () in
        Lwt.return ()
      )
    in

    loop true


end


(* ends here *)
