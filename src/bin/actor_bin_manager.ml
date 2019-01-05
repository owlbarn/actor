(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Manager = Actor_manager.Make(Actor_net_zmq) (Actor_sys_unix)


let main () =
  Manager.run Actor_config.manager_addr


let _ = Lwt_main.run (main ())
