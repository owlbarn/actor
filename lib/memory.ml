(** [ Dfs ]
  is a distributed file system for actors
*)

open Types

type t = Obj.t

let _data = ref StrMap.empty

let rand_id () = string_of_int (Random.int 536870912)

let mem id = StrMap.mem id !_data

let add id d =
  _data := StrMap.add id (Obj.repr d) !_data

let remove id = StrMap.remove id !_data

let find id = Obj.obj (StrMap.find id !_data)

let size id = Obj.size (StrMap.find id !_data)

(* FIXME: debug purpose *)

let _ =
  let d = Array.init 5 (fun x -> Random.float 10.) in
  add "default" d
