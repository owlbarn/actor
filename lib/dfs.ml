(** [ Dfs ]
  is a distributed file system for actors
*)

(* TODO: this module needs some redesign ... *)

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

(* TODO: design a simple fs *)
let list path =
  let elements = Str.split (Str.regexp "/") path in
  match elements with
    | hd :: tl -> ()
    | [] -> ()

let size did = (StrMap.find did !_data).size
