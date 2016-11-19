(** [ Peer-to-Peer Parallel ]  *)

open Types

(** start running the model loop *)
val start : string -> string -> unit

(** register user-defined barrier function at p2p server *)
val register_barrier : ('a list -> bool) -> unit

(** register user-defined pull function at p2p server *)
val register_pull : (('a * 'b * int) list -> ('a * 'b * int) list) -> unit

(** register user-defined scheduler at p2p client *)
val register_schedule : (string -> 'a list) -> unit

(** register user-defined push function at p2p client *)
val register_push : (string -> ('a * 'b) list -> ('a * 'b) list) -> unit

(** register stopping criterion function *)
val register_stop : (unit -> bool) -> unit

(** given a key, get its value and timestamp *)
val get : 'a -> 'b * int

(** given a key, set its value at master *)
val set : 'a -> 'b -> unit
