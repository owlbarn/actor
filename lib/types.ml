(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_status = Available | Unavailable

type message_type =
  | User_Reg
  | Job_Reg

type message_rec = {
  typ : message_type;
  str : string;
}

type actor_rec = {
  id : string;
  status : actor_status;
  last_seen : float;
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

let to_msg t s =
  let m = { typ = t; str = s } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m
