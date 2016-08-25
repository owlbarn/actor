(** [ Dfs ]
  is a distributed file system for actors
*)

open Types

type t = Obj.t

let _data = Hashtbl.create 1048576

let rand_id () = string_of_int (Random.int 536870912)

let mem id = Hashtbl.mem _data id

let add id d = Hashtbl.add _data id (Obj.repr d)

let remove id = Hashtbl.remove _data id

let find id = Obj.obj (Hashtbl.find _data id)

let size id = Obj.size (find id)

let load filename = None

(* FIXME: debug purpose *)

let _ =
  let d = Array.init 5 (fun x -> Random.float 10.) in
  add "default" (Array.to_list d)
