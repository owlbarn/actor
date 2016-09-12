(** [ Storage module ]
  provide a basic persistent storage service
*)

open Lwt
open Cohttp
open Cohttp_lwt_unix

(** the following functions are for native Unix storage *)

let unix_load x =
  let l = Unix.((stat x).st_size) in
  let b = Bytes.create l in
  let f = Unix.(openfile x [O_RDONLY] 0o644) in
  let _ = Unix.read f b 0 l in b

let unix_save x b =
  let f = Unix.(openfile x [O_WRONLY; O_CREAT] 0o644) in
  let l = Bytes.length b in
  Unix.write f b 0 l


(** the following functions are for HDFS storage *)

let _hdfs_base = "http://" ^ Config.webhdfs_addr ^ "/webhdfs/v1"

let get_file_json uri =
  Client.get (Uri.of_string uri) >>= fun (resp, body) ->
  Cohttp_lwt_body.to_string body

let get_file_info s = None

let hdfs_load x =
  let uri = _hdfs_base ^ x in
  uri

let hdfs_save x = None


(** the following functions are for Irmin storage *)

let irmin_load x = None

let irmin_save x = None


(** FIXME: for debug purpose *)

let _ = Logger.debug "%s" (hdfs_load "/tmp/hello.txt")
