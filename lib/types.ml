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
  | MapTask | FilterTask | ReduceByKeyTask | ShuffleTask | UnionTask
  | JoinTask | FlattenTask | ApplyTask | NopTask
  | Pipeline | Collect | Count | Broadcast | Fold | Reduce | Terminate | Load | Save

type message_rec = {
  bar : int;
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

let to_msg b t p =
  let m = { bar = b; typ = t; par = p } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m;;

(* initialise some states *)

Random.self_init ();;
