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
  | PS_Get | PS_Set | PS_Schedule | PS_Push

type message_rec = {
  bar : int;
  typ : message_type;
  par : string array;
}

type context = {
  mutable jid : string;
  mutable master : string;
  mutable worker : [`Dealer] ZMQ.Socket.t StrMap.t;
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

(** types of user-defined functions in model parallel module *)

type ('a, 'b, 'c) ps_schedule_typ = 'a list -> ('a * ('b * 'c) list) list

type ('a, 'b, 'c) ps_pull_typ = ('a * 'b) list -> ('a * 'c) list

type ('a, 'b, 'c) ps_push_typ = 'a -> ('b * 'c) list -> ('b * 'c) list

(** two functions to translate between message rec and string *)

let to_msg b t p =
  let m = { bar = b; typ = t; par = p } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m;;

(* initialise some states *)

Random.self_init ();;
