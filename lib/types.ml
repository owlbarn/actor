(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type actor_status = Available | Unavailable

type message_type =
  | User_Reg
  | Data_Reg
  | Job_Reg
  | Heartbeat

type message_rec = {
  typ : message_type;
  uid : string;
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

let to_msg t i s =
  let m = { typ = t; uid = i; str = s } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m;;

(* initialise some states *)

Random.self_init ();;
