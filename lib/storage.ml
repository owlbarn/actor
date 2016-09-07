(** [ Storage ]
  provide a basic persistent storage service
*)

open Lwt
open Irmin_unix

module Store = Irmin_git.FS (Irmin.Contents.String)(Irmin.Ref.String)(Irmin.Hash.SHA1)

let _conf = Irmin_git.config ~root:"./irmin.data" ~bare:true ()
let _repo = Store.Repo.create _conf >>= Store.master task

let load x =
  let y = _repo >>= fun t -> Store.read_exn (t "Reading ...") [x] in
  Lwt_main.run y

let save x s =
  let y = _repo >>= fun t -> Store.update (t "Updating ...")  [x] s in
  Lwt_main.run y

let test () =
  _repo >>= fun t ->
  Store.update (t "Updating foo/bar")  ["foo"; "bar"] "hi!" >>= fun () ->
  Store.read_exn (t "Reading foo/bar") ["foo"; "bar"] >>= fun x ->
  Printf.printf "Read: %s\n%!" x;
  Lwt.return_unit

let t1 () = Lwt_main.run (test ())

let _ =
  let _ = save "k1" "hello" in
  let _ = save "k2" "world" in
  let v1 = load "k1" in
  let v2 = load "k2" in
  print_endline (v2 ^ " " ^ v1)
