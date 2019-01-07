(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make
  (Net : Actor_net.Sig)
  (Sys : Actor_sys.Sig)
  = struct

  open Actor_mapre_types

  module Server = Actor_mapre_server.Make (Net) (Sys)

  module Client = Actor_mapre_client.Make (Net) (Sys)


  let init contex =
    let uuid = contex.myself in
    let addr = Hashtbl.find contex.book uuid in

    if contex.myself = contex.server then (
      Owl_log.debug "mapre server %s @ %s" uuid addr;
      Server.init contex
    )
    else (
      Owl_log.debug "mapre client %s @ %s" uuid addr;
      Client.init contex
    )



  (* interface to mapreserver functions *)

end
