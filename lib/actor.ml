(** [ Actor ]
  defines basic functionality of an actor
*)

open Types

let test () = print_endline "I am an actor."

let create id = { id = id; last_seen = 0. }

let register id = None

let register_data x = None

let heartbeat x = None
