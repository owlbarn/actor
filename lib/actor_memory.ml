(** [ Dfs ]
  is a distributed file system for actors
*)

open Actor_types

let _data : (string, Obj.t) Hashtbl. t = Hashtbl.create 10_000_000

let rand_id () = string_of_int (Random.int 536_870_912)

let mem id = Hashtbl.mem _data id

let add id d = Hashtbl.add _data id (Obj.repr d)

let remove id = Hashtbl.remove _data id

let find id = match id with
  | "" -> Obj.obj (Obj.repr [ None ])
  | _  -> Obj.obj (Hashtbl.find _data id)

let size id = Obj.size (find id)
