(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

open Actor_param_types


let is_ready context =
  Hashtbl.iter (fun k v ->
    Owl_log.debug "info --- %s %s" k v
  ) context.book
