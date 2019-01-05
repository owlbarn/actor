(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Worker = Actor_worker.Make(Actor_net_zmq)

let _ =
  let myid = "worker_" ^ (string_of_int (Random.int 9000 + 1000)) in
  let addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000)) in
  Worker.run myid addr Actor_config.manager_addr
