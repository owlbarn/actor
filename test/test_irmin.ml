(** [ Test Irmin ]  *)

open Lwt

module Store = Irmin_mem.AO (Irmin.Hash.SHA1) (Irmin.Contents.String)

let config = Irmin_mem.config ()

let test () =
  let s = Store.create config
  >>= fun s -> Store.add s "hello"
  >>= fun k -> Store.read s k
  >>= function
    | Some v -> print_endline v; Lwt.return_unit
    | None -> print_endline "None"; Lwt.return_unit
  in s

let _ = Lwt_main.run (test ())
