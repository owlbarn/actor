(** [ Model Parallel ] Parameter server module  *)

open Actor_types

(** start running the model loop *)
val start : string -> string -> unit

(** register user-defined barrier function at p2p server *)
val register_barrier : ps_barrier_typ -> unit

(** register user-defined scheduler *)
val register_schedule : ('a, 'b, 'c) ps_schedule_typ -> unit

(** register user-defined pull function executed at master *)
val register_pull : ('a, 'b, 'c) ps_pull_typ -> unit

(** register user-defined push function executed at worker *)
val register_push : ('a, 'b, 'c) ps_push_typ -> unit

(** register stopping criterion function *)
val register_stop : ps_stop_typ -> unit

(** given a key, get its value and timestamp *)
val get : 'a -> 'b * int

(** given a key, set its value at master *)
val set : 'a -> 'b -> unit

(** FIXME: reture all the keys in a parameter server *)
val keys : unit -> 'a list
