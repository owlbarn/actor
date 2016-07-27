(** [ Actor ]
  defines basic functionality of an actor
*)

open Types

let test () = print_endline "I am an actor."

let _actors = ref StrMap.empty

(** fucntions for manager  *)

let create id = { id = id; status = Available; last_seen = Unix.time () }

let add id = _actors := StrMap.add id (create id) !_actors

let remove id = None

let mem id = StrMap.mem id !_actors

let update id = None

(** functions for nodes  *)

let register id = None

let register_data x = None

let heartbeat x = None
