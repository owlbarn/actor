(** [ Dfs ]
  is a distributed file system for actors
*)

open Types

type t = {
  did : string;  (* data id *)
  uid : string;  (* user id *)
  size : int;    (* size in bytes *)
}

let _data = ref StrMap.empty

let add did uid =
  _data := StrMap.add did {
    did = did; uid = uid; size = 0
  } !_data

let mem did = StrMap.mem did !_data

let remove did = StrMap.remove did !_data

let list x = None

let size did = (StrMap.find did !_data).size
