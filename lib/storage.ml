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
  modificationTime : int;
  blockSize : int;
  childrenNum : int;
  fileId : int;
  group : string;
  length : int;
  owner : string;
  pathSuffix : string;
  permission : string;
  replication : int;
  storagePolicy : int;
  ftyp : string;
}

let _hdfs_base = "http://" ^ Config.webhdfs_addr ^ "/webhdfs/v1"

let get_file_json uri =
  Client.get (Uri.of_string uri) >>= fun (resp, body) ->
  Cohttp_lwt_body.to_string body

let get_file_info s =
  let open Yojson.Basic.Util in
  let json = Yojson.Basic.from_string s in
  let temp = json |> member "FileStatuses" |> member "FileStatus" |> to_list in
  let stat = List.nth temp 0 in
  let info = {
    accessTime = stat |> member "accessTime" |> to_int;
    modificationTime = stat |> member "modificationTime" |> to_int;
    blockSize = stat |> member "blockSize" |> to_int;
    childrenNum = stat |> member "childrenNum" |> to_int;
    fileId = stat |> member "fileId" |> to_int;
    group = stat |> member "group" |> to_string;
    length = stat |> member "length" |> to_int;
    owner = stat |> member "owner" |> to_string;
    pathSuffix = stat |> member "pathSuffix" |> to_string;
    permission = stat |> member "permission" |> to_string;
    replication = stat |> member "replication" |> to_int;
    storagePolicy = stat |> member "storagePolicy" |> to_int;
    ftyp = stat |> member "type" |> to_string;
  } in info

let hdfs_load x =
  let uri = _hdfs_base ^ x ^ "?op=LISTSTATUS" in
  let jstr = get_file_json uri |> Lwt_main.run in
  let info = get_file_info jstr in
  let uri = _hdfs_base ^ x ^ "?op=OPEN" in
  let _ = Logger.info "%s" uri in
  let s = get_file_json uri |> Lwt_main.run in
  s

let hdfs_save x = None


(** the following functions are for Irmin storage *)

let irmin_load x = None

let irmin_save x = None


(** FIXME: for debug purpose *)

let _ =
  let s = hdfs_load "/tmp/hello.txt" in
  print_endline s
