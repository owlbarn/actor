(** [ Barrier module ]
  provides flexible synchronisation barrier controls.
*)

open Types

(** Bulk synchronous parallel *)
let bsp bar router workers msgbuf =
  let h = Hashtbl.create 1024 in
  (** first check the buffer for those arrive early *)
  List.iter (fun (i,m) ->
    if not (Hashtbl.mem h i) then Hashtbl.(add h i m; remove msgbuf bar)
  ) (Hashtbl.find_all msgbuf bar);
  (** then wait for the rest of the messages *)
  while (Hashtbl.length h) < (StrMap.cardinal workers) do
    let i, m = Utils.recv router in
    if bar = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m;
  done;
  Hashtbl.fold (fun k v l -> v :: l) h []

(** Stale synchronous parallel *)
let ssp = None
