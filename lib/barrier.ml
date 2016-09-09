(** [ Barrier module ]
  provides flexible synchronisation barrier controls.
*)

let _bcache = Hashtbl.create 1_000_000
let _fcache = Hashtbl.create 1_000_000

let add bar res = Hashtbl.add _bcache bar res

let create_bar f =
let bar = Random.int 536870912 in
Hashtbl.add _fcache bar f; bar

let _ =
  Hashtbl.add _bcache 1 "hello";
  Hashtbl.add _fcache 1 "hello"
