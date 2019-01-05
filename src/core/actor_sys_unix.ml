(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

let exec cmd _args =
  print_endline ("Actor_sys_unix.exec" ^ cmd)


let file_exists = Lwt_unix.file_exists


let sleep = Lwt_unix.sleep
