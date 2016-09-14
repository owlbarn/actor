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

let _get_by_uri uri = Client.get (Uri.of_string uri)
let _put_by_uri uri = Client.put (Uri.of_string uri)
let _post_by_uri s uri = Client.post ~body:(Cohttp_lwt_body.of_string s) (Uri.of_string uri)

let _get_body t = (
  t >>= fun (resp, body) -> Cohttp_lwt_body.to_string body )
  |> Lwt_main.run

let _get_location t = (
  t >>= fun (resp, body) ->
  match (resp |> Response.headers |> Header.get_location) with
  | Some x -> return (Uri.to_string x) | None -> return "" )
  |> Lwt_main.run

let get_file_info x =
  let open Yojson.Basic.Util in
  let stat = _hdfs_base ^ x ^ "?op=GETFILESTATUS"
  |> _get_by_uri |> _get_body |> Yojson.Basic.from_string
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

let get_file_content x o l =
  let content = _hdfs_base ^ x ^ "?op=OPEN" ^
    "&offset=" ^ (string_of_int o) ^ "&length=" ^ (string_of_int l)
  |> _get_by_uri |> _get_location |> _get_by_uri |> _get_body in
  content

let hdfs_load x =
  let info = get_file_info x in
  get_file_content x 0 info.length

let hdfs_save x b =
  let _ = _hdfs_base ^ x ^ "?op=CREATE"
  |> _put_by_uri |> _get_location |> _put_by_uri |> _get_location in
  Unix.sleep 1;  (** FIXME: sleep ... *)
  let s = _hdfs_base ^ x ^ "?op=APPEND"
  |> _post_by_uri "" |> _get_location |> _post_by_uri b |> _get_body in s

(** the following functions are for Irmin storage *)

module Irmin_Storage =
  Irmin_unix.Irmin_git.FS
    (Irmin.Contents.String)(Irmin.Ref.String)(Irmin.Hash.SHA1)

let irmin_handle = ref None

let get_store () =
  match !irmin_handle with
  | Some s -> return s
  | None ->
     let config = Irmin_unix.Irmin_git.config ~root:"." () in
     Irmin_Storage.Repo.create config
     >>= Irmin_Storage.master Irmin_unix.task
     >>= fun t ->
     let () = irmin_handle := Some t in
     return t


let irmin_load x =
  (get_store () >>= fun s ->
   let s = s ("load " ^ x) in
   Irmin_Storage.read s [x] >>= function
   | None -> fail Not_found
   | Some v -> return v)
  |> Lwt_main.run


let irmin_save x b =
  (get_store () >>= fun s ->
   let s = s ("save " ^ x) in
   Irmin_Storage.update s [x] b >>= fun () ->
   return @@ Bytes.length b)
  |> Lwt_main.run



(** FIXME: for debug purpose *)

let _ =
  Logger.debug "%s" (hdfs_load "/tmp/hello.txt");
  Logger.debug "%s" (hdfs_save "/tmp/world.txt" "test writing to a file\n")
