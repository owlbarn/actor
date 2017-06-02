(** [ Peer-to-Peer Parallel ]  *)

open Actor_types

(** start running the model loop *)
val start : string -> string -> unit

(** register user-defined barrier function at p2p server *)
val register_barrier : p2p_barrier_typ -> unit

(** register user-defined pull function at p2p server *)
val register_pull : ('a, 'b) p2p_pull_typ -> unit

(** register user-defined scheduler at p2p client *)
val register_schedule : 'a p2p_schedule_typ -> unit

(** register user-defined push function at p2p client *)
val register_push : ('a, 'b) p2p_push_typ -> unit

(** register stopping criterion function at p2p client *)
val register_stop : p2p_stop_typ -> unit

(** given a key, get its value and timestamp *)
val get : 'a -> 'b * int

(** given a key, set its value at master *)
val set : 'a -> 'b -> unit
