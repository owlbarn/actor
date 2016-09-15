(** [ Parameter Server ]
  provides a global variable like kv store
*)

Logger.update_config Config.level Config.logdir ""

let _param = Hashtbl.create 1_000_000

let get k t =
  let v, t' = Hashtbl.find _param (Obj.repr k) in
  Logger.debug "%i" (t' - t);
  v

let set k v t =
  let v, t' = Hashtbl.find _param (Obj.repr k) in
  Logger.debug "%i" (t' - t);
  Hashtbl.replace _param (Obj.repr k) (v, t)

let schedule x = None


(** FIXME: for debug purpose *)

let _ =
  Hashtbl.add _param (Obj.repr 5) ("abc", 123);
  Hashtbl.add _param (Obj.repr 6) ("def", 125);
  let _ = get 5 124 in
  let _ = get 6 123 in
  let _ = set 6 "ccc" 126 in
  ()
