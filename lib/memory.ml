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

let load id fname =
  let l = Unix.((stat fname).st_size) in
  let b = Bytes.create l in
  let f = Unix.openfile fname [] 0o644 in
  let _ = Unix.read f b 0 l in
  add id [ b ]

(* FIXME: debug purpose *)

let _ =
  let d = Array.init 5000 (fun x -> Random.float 10.) in
  add "default" (Array.to_list d);
  load "wordcount" "./data/wordcount.data"
