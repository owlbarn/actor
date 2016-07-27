(** [ Actor ]
  defines basic functionality of an actor
*)

open Types


let test () = print_endline "I am an actor."

(** fucntions for manager  *)

let create id = { id = id; status = Available; last_seen = Unix.time () }

let add id = None

let remove id = None

let mem id = None

let update id = None

(** functions for nodes  *)

let register id = None

let register_data x = None

let heartbeat x = None
