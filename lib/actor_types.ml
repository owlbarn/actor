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
  | OK | Fail | Heartbeat | User_Reg | Job_Reg | Job_Master | Job_Worker | Job_Create
  (* Data parallel: Mapre *)
  | MapTask | MapPartTask | FilterTask | ReduceByKeyTask | ShuffleTask | UnionTask
  | JoinTask | FlattenTask | ApplyTask | NopTask
  | Pipeline | Collect | Count | Broadcast | Fold | Reduce | Terminate | Load | Save
  (* Model Parallel: Param *)
  | PS_Get | PS_Set | PS_Schedule | PS_Push
  (* P2P Parallel: Peer *)
  | P2P_Reg | P2P_Connect | P2P_Ping | P2P_Join | P2P_Forward | P2P_Set | P2P_Get
  | P2P_Get_Q | P2P_Get_R | P2P_Copy | P2P_Push | P2P_Pull | P2P_Pull_Q | P2P_Pull_R
  | P2P_Bar

type message_rec = {
  mutable bar : int;
  mutable typ : message_type;
  mutable par : string array;
}

type mapre_context = {
  mutable ztx         : ZMQ.Context.t;                    (* zmq context for communication *)
  mutable job_id      : string;                           (* job id or swarm id, depends on paradigm *)
  mutable master_addr : string;                           (* different meaning in different paradigm *)
  mutable myself_addr : string;                           (* communication address of current process *)
  mutable master_sock : [`Dealer] ZMQ.Socket.t;           (* socket of master_addr *)
  mutable myself_sock : [`Router] ZMQ.Socket.t;           (* socket of myself_addr *)
  mutable workers     : [`Dealer] ZMQ.Socket.t StrMap.t;  (* socket of workers or peers *)
  mutable step        : int;                              (* local step for barrier control *)
  mutable msbuf       : (int, string * message_rec) Hashtbl.t;  (* buffer of un-ordered messages *)
}

type param_context = {
  mutable ztx         : ZMQ.Context.t;                    (* zmq context for communication *)
  mutable job_id      : string;                           (* job id or swarm id, depends on paradigm *)
  mutable master_addr : string;                           (* different meaning in different paradigm *)
  mutable myself_addr : string;                           (* communication address of current process *)
  mutable master_sock : [`Dealer] ZMQ.Socket.t;           (* socket of master_addr *)
  mutable myself_sock : [`Router] ZMQ.Socket.t;           (* socket of myself_addr *)
  mutable workers     : [`Dealer] ZMQ.Socket.t StrMap.t;  (* socket of workers or peers *)
  mutable step        : int;                              (* local step for barrier control *)
  mutable stale       : int;                              (* staleness variable for barrier control *)
  mutable worker_busy : (string, int) Hashtbl.t;          (* lookup table of a worker busy or not *)
  mutable worker_step : (string, int) Hashtbl.t;          (* lookup table of a worker's step *)
  mutable step_worker : (int, string) Hashtbl.t;          (* lookup table of workers at a specific step *)
}

type peer_context = {
  mutable ztx         : ZMQ.Context.t;                    (* zmq context for communication *)
  mutable job_id      : string;                           (* job id or swarm id, depends on paradigm *)
  mutable master_addr : string;                           (* different meaning in different paradigm *)
  mutable myself_addr : string;                           (* communication address of current process *)
  mutable master_sock : [`Dealer] ZMQ.Socket.t;           (* socket of master_addr *)
  mutable myself_sock : [`Router] ZMQ.Socket.t;           (* socket of myself_addr *)
  mutable workers     : [`Dealer] ZMQ.Socket.t StrMap.t;  (* socket of workers or peers *)
  mutable step        : int;                              (* local step for barrier control *)
  mutable block       : bool;                             (* is client blocked at barrier *)
  mutable mpbuf       : Obj.t list;                       (* buffer of model parameter updates *)
  mutable spbuf       : (string, int) Hashtbl.t;          (* buffer of the step of connected peers, piggybacked in m.bar *)
}

type actor_rec = {
  id        : string;
  addr      : string;
  last_seen : float;
}

type data_rec = {
  id    : string;
  owner : string;
}

type service_rec = {
  id             : string;
  master         : string;
  mutable worker : string array;
}

(** types of user-defined functions in model parallel module *)

type ('a, 'b, 'c) ps_schedule_typ = 'a list -> ('a * ('b * 'c) list) list

type ('a, 'b, 'c) ps_pull_typ = ('a * 'b) list -> ('a * 'c) list

type ('a, 'b, 'c) ps_push_typ = 'a -> ('b * 'c) list -> ('b * 'c) list

type ps_barrier_typ = param_context ref -> int * (string list)

type ps_stop_typ = param_context ref -> bool

(** types of user-defined functions in p2p parallel module *)

type 'a p2p_schedule_typ = peer_context ref -> 'a list

type ('a, 'b) p2p_pull_typ = peer_context ref -> ('a * 'b * int) list -> ('a * 'b * int) list

type ('a, 'b) p2p_push_typ = peer_context ref -> ('a * 'b) list -> ('a * 'b) list

type p2p_barrier_typ = peer_context ref -> bool

type p2p_stop_typ = peer_context ref -> bool

(** two functions to translate between message rec and string *)

let to_msg b t p =
  let m = { bar = b; typ = t; par = p } in
  Marshal.to_string m [ ]

let of_msg s =
  let m : message_rec = Marshal.from_string s 0 in m;;

(* initialise some states *)

Random.self_init ();;
