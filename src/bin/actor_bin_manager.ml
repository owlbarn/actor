(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


module Manager = Actor_manager.Make(Actor_net_zmq)

let _ =
  Manager.run Actor_config.manager_addr
