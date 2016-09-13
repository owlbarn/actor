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

type hdfs_info = {
  accessTime : int;
  blockSize : int;
  group : string;
  length : int;
  modificationTime : int;
  owner : string;
  pathSuffix : string;
  permission : string;
  replication : int;
  ftyp : string;
}

let _hdfs_base = "http://" ^ Config.webhdfs_addr ^ "/webhdfs/v1"

let _get_body_string uri = (
  Client.get (Uri.of_string uri) >>= fun (resp, body) ->
  Cohttp_lwt_body.to_string body )
  |> Lwt_main.run

let _get_header_location uri = (
  Client.get (Uri.of_string uri) >>= fun (resp, body) ->
  match (resp |> Response.headers |> Header.get_location) with
  | Some x -> return (Uri.to_string x) | None -> return "" )
  |> Lwt_main.run

let _put_header_location uri = (
  Client.put (Uri.of_string uri) >>= fun (resp, body) ->
  match (resp |> Response.headers |> Header.get_location) with
  | Some x -> return (Uri.to_string x) | None -> return "" )
  |> Lwt_main.run

let get_file_info x =
  let open Yojson.Basic.Util in
  let stat = _hdfs_base ^ x ^ "?op=GETFILESTATUS"
  |> _get_body_string |> Yojson.Basic.from_string
  |> member "FileStatus" in
  let info = {
    accessTime = stat |> member "accessTime" |> to_int;
    blockSize = stat |> member "blockSize" |> to_int;
    group = stat |> member "group" |> to_string;
    length = stat |> member "length" |> to_int;
    modificationTime = stat |> member "modificationTime" |> to_int;
    owner = stat |> member "owner" |> to_string;
    pathSuffix = stat |> member "pathSuffix" |> to_string;
    permission = stat |> member "permission" |> to_string;
    replication = stat |> member "replication" |> to_int;
    ftyp = stat |> member "type" |> to_string;
  } in info

let get_file_content x =
  let content = _hdfs_base ^ x ^ "?op=OPEN"
  |> _get_header_location |> _get_body_string in
  content

let hdfs_load x =
  let info = get_file_info x in
  Logger.debug "size = %i" info.length;
  get_file_content x

let hdfs_save x =
  let loc = _hdfs_base ^ x ^ "?op=CREATE"
  |> _put_header_location in loc


(** the following functions are for Irmin storage *)

let irmin_load x = None

let irmin_save x = None


(** FIXME: for debug purpose *)

let _ =
  Logger.debug "%s" (hdfs_load "/tmp/hello.txt");
  Logger.debug "%s" (hdfs_save "/tmp/world.txt")
