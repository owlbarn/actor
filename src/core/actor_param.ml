(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_param_types

  module Server = Actor_param_server.Make (Net) (Sys)

  module Client = Actor_param_client.Make (Net) (Sys)


  let init context =
    let uuid = context.myself in
    let addr = Hashtbl.find context.book uuid in

    if context.myself = context.server then (
      Owl_log.debug "param server %s @ %s" uuid addr;
      Server.init context
    )
    else (
      Owl_log.debug "param client %s @ %s" uuid addr;
      Client.init context
    )



  (* interface to paramserver functions *)

end
