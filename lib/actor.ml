(** [ Actor ]
  defines basic functionality of an actor
*)

open Types

let _actors = ref StrMap.empty

(** fucntions for manager  *)

let create id addr = {
  id = id;
  addr = addr;
  status = Available;
  last_seen = Unix.time ()
  }

let add id addr = _actors := StrMap.add id (create id addr) !_actors

let remove id = _actors := StrMap.remove id !_actors

let mem id = StrMap.mem id !_actors

let to_list () = StrMap.fold (fun k v l -> l @ [v]) !_actors []

let addrs () = StrMap.fold (fun k v l -> l @ [v.addr]) !_actors []

(** functions for nodes  *)
