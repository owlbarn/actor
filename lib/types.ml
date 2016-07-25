(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_rec = {
  id : string;
  last_seen : float;
}

(* TODO: message format *)
