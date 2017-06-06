(** [ Data Parallel ] Map-Reduce module *)

val init : string -> string -> unit

val map : ('a -> 'b) -> string -> string

val map_partition : ('a list -> 'b list) -> string -> string

val flatmap : ('a -> 'b list) -> string -> string

val reduce : ('a -> 'a -> 'a) -> string -> 'a option

val reduce_by_key : ('a -> 'a -> 'a) -> string -> string

val fold : ('a -> 'b -> 'a) -> 'a -> string -> 'a

val filter : ('a -> bool) -> string -> string

val flatten : string -> string

val shuffle : string -> string

val union : string -> string -> string

val join : string -> string -> string

val broadcast : 'a -> string

val get_value : string -> 'a

val count : string -> int

val collect : string -> 'a list

val terminate : unit -> unit

val apply : ('a list -> 'b list) -> string list -> string list -> string list

val load : string -> string

val save : string -> string -> int

(* experimental functions *)

val workers : unit -> string list

val myself : unit -> string


(** TODO: sample function *)
