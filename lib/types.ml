(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_status = Available | Unavailable

type actor_rec = {
  id : string;
  status : actor_status;
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
