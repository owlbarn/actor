(** [ Config module ]
  contains all the configurations of the framework.
*)

(** Manager's address, all workders connect to this address *)
let manager_addr = "tcp://127.0.0.1:5555"
(** Manager's identifier *)
let manager_id = "manager_0"

(** File system path, Irmin or HDFS *)
let dfs_path = "storage.data"

(** Log configs: path, level, color, etc. *)
let level = Logger.DEBUG
let logdir = "log/"
let output = stderr

(** update logging config if above params change *)
let update_logger fname level =
  Logger.set_log_level level;
  match fname with
  | "" -> Logger.color_on (); Logger.set_output output
  | _  -> Logger.color_off (); open_out (logdir^fname) |> Logger.set_output

(** init the config module *)
let _ = update_logger "" level
