(** [ Dfs ]
  is a distributed file system for actors
*)

open Types

type t = Obj.t

let _data = ref StrMap.empty

let add id data = _data := StrMap.add id data !_data

let mem id = StrMap.mem id !_data

let remove id = StrMap.remove id !_data

let size id = Obj.size (StrMap.find id !_data)
