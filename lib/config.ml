(** [ Config module ]
  contains all the configurations of the framework.
*)

(** Manager's address, all workders connect to this address *)
let manager_addr = "tcp://127.0.0.1:5555"
(** Manager's identifier *)
let manager_id = "manager_0"

(** File system path, Irmin or HDFS *)
let dfs_path = "storage.data"
