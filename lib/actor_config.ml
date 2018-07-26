(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Config: contains the static configurations of the framework. *)


(** Manager's address, all workders connect to this address *)
let manager_addr = "tcp://127.0.0.1:5555"


(** Manager's identifier *)
let manager_id = "manager_0"


(** File system path, Irmin or HDFS *)
let dfs_path = "storage.data"


(** Log configs: path, level, color, etc. *)
let update_config level logdir fname =
  Owl_log.set_level level;
  match fname with
  | "" -> (Owl_log.set_color true;  Owl_log.set_output stderr)
  | _  -> (Owl_log.set_color false; open_out (logdir^fname) |> Owl_log.set_output)

let logdir = "log/"
let _ =  update_config Owl_log.DEBUG logdir ""


(** Max queue length of ZMQ send and receive *)
let high_warter_mark = 10_000


(** WebHDFS base addr and port *)
let webhdfs_addr = "192.168.99.100:50070"
