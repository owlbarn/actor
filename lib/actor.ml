(** [ Actor ]
  defines basic functionality of an actor
*)

open Types

let test () = print_endline "I am an actor."

let create id = { id = id; last_seen = 0. }
