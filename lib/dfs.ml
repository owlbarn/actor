(** [ Dfs ]
  is a distributed file system for actors
*)

(* TODO: this module needs some redesign ...
  think about how to abstract distributed datasets, compare to rdd
*)

open Types

type t = {
  did : string;            (* data id *)
  uid : string;            (* user id *)
  size : int;              (* size in bytes *)
  service : string list;   (* associated services *)
}

let _data = ref StrMap.empty

let add did uid =
  _data := StrMap.add did {
    did = did; uid = uid; size = 0;
    service = [];
  } !_data

let mem did = StrMap.mem did !_data

let remove did = StrMap.remove did !_data

let size did = (StrMap.find did !_data).size
