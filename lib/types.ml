(** [ Types ]
  includes the types shared by different modules.
*)

module StrMap = struct
  include Map.Make (String)
  let keys x = List.map fst (bindings x)
  let values x = List.map snd (bindings x)
end

type color = Red | Green | Blue

type message_type =
  | OK | Fail | Heartbeat
  | User_Reg | Data_Reg
  | Job_Reg | Job_Master | Job_Worker | Job_Create
  | MapTask | FilterTask | UnionTask | ShuffleTask | ReduceTask
  | Pipeline | Collect | Count | Broadcast | Fold | Terminate

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
