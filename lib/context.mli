(** [ Context module ]  *)

val init : string -> string -> unit

val map : ('a -> 'b) -> string -> string

val reduce : ('a -> 'a -> 'a) -> string -> string

val fold : ('a -> 'b -> 'a) -> 'a -> string -> 'a

val filter : ('a -> bool) -> string -> string

val flatten : string -> string

val shuffle : string -> string

val union : string -> string -> string

val broadcast : 'a -> string

val get_value : string -> 'a

val count : string -> int

val collect : string -> 'a list

val terminate : unit -> unit
