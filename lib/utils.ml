(** [
  Some shared helper functions
]  *)

open Types

let logger s =
  let open Unix in
  let t = gmtime (time ()) in
  Printf.printf "[#%i %02i:%02i:%02i] %s\n%!" (getpid ()) t.tm_hour t.tm_min t.tm_sec s

let group_by_key x =
  let h, g = Hashtbl.(create 1024, create 1024) in
  List.iter (fun (k,v) -> Hashtbl.(add h k v; if not (mem g k) then add g k None)) x;
  Hashtbl.fold (fun k v l -> (k,(List.map (fun v -> (k,v)) Hashtbl.find_all h k)) :: l) g []

let flatten_kvg x = List.map (fun (k,l) -> List.map (fun v -> (k,v)) l) x |> List.flatten
