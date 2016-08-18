(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = Map.Make (String)

type color = Red | Green | Blue

type message_type =
  | User_Reg
  | Data_Reg
  | Heartbeat
  | Job_Reg
  | Job_Master
  | Job_Worker
  | Job_Create
  | MapTask
  | FilterTask
  | UnionTask
  | PipelinedTask
  | CollectTask
  | CountTask
  | BroadcastTask
  | Terminate

type message_rec = {
  typ : message_type;
  par : string array;
}

type actor_rec = {
  id : string;
  addr : string;
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

let to_msg t p =
  let m = { typ = t; par = p } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m;;

(* initialise some states *)

Random.self_init ();;
