(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_status = Available | Unavailable

type message_type =
  | User_Reg
  | Job_Reg

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

type service_rec = {
  id : string;
  master : string;
  mutable workers : string list;
}
