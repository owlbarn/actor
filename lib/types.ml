(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_rec = {
  id : string;
  last_seen : float;
}

type message_rec = {
  typ : int;
  content : string;
}

type data_rec = {
  id : string;
  owner : string;
}
