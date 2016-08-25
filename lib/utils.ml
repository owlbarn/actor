(** [
  Some shared helper functions
]  *)

open Types

let logger s =
  let open Unix in
  let t = gmtime (time ()) in
  Printf.printf "[#%i %02i:%02i:%02i] %s\n%!" (getpid ()) t.tm_hour t.tm_min t.tm_sec s

let rec _bind_available_addr addr router ztx =
  addr := "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000));
  try ZMQ.Socket.bind router !addr
  with exn -> _bind_available_addr addr router ztx

let bind_available_addr ztx =
  let router : [`Router] ZMQ.Socket.t = ZMQ.Socket.create ztx ZMQ.Socket.router in
  let addr = ref "" in _bind_available_addr addr router ztx;
  !addr, router

(* the following 3 functions are for shuffle operations *)
let group_by_key x =
  let h, g = Hashtbl.(create 1024, create 1024) in
  List.iter (fun (k,v) -> Hashtbl.(add h k v; if not (mem g k) then add g k None)) x;
  Hashtbl.fold (fun k _ l -> (k,Hashtbl.find_all h k) :: l) g []

let flatten_kvg x = List.map (fun (k,l) -> List.map (fun v -> (k,v)) l) x |> List.flatten

let choose_load x n i = List.filter (fun (k,l) -> (Hashtbl.hash k mod n) = i) x
