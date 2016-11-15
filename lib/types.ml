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
  (* General messge types *)
  | OK | Fail | Heartbeat
  | User_Reg | Data_Reg
  | Job_Reg | Job_Master | Job_Worker | Job_Create
  (* Data parallel: Mapre *)
  | MapTask | FilterTask | ReduceByKeyTask | ShuffleTask | UnionTask
  | JoinTask | FlattenTask | ApplyTask | NopTask
  | Pipeline | Collect | Count | Broadcast | Fold | Reduce | Terminate | Load | Save
  (* Model Parallel: Param *)
  | PS_Get | PS_Set | PS_Schedule | PS_Push
  (* P2P Parallel: Peer *)
  | P2P_Reg | P2P_Ping | P2P_Forward | P2P_Connect | P2P_Get | P2P_Set

type message_rec = {
  bar : int;
  typ : message_type;
  par : string array;
}

type context = {
  mutable job_id      : string;
  mutable master_addr : string;
  mutable myself_addr : string;
  mutable master_sock : [`Dealer] ZMQ.Socket.t;
  mutable myself_sock : [`Router] ZMQ.Socket.t;
  mutable workers     : [`Dealer] ZMQ.Socket.t StrMap.t;
  mutable ztx         :  ZMQ.Context.t
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
  mutable worker : string array;
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
