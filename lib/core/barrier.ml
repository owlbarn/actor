(** [ Barrier module ]
  provides flexible synchronisation barrier controls.
*)

open Types

(* Bulk synchronous parallel *)
let bsp bar router workers msgbuf =
  let h = Hashtbl.create 1024 in
  (* first check the buffer for those arrive early *)
  List.iter (fun (i,m) ->
    if not (Hashtbl.mem h i) then Hashtbl.(add h i m; remove msgbuf bar)
  ) (Hashtbl.find_all msgbuf bar);
  (* then wait for the rest of the messages *)
  while (Hashtbl.length h) < (StrMap.cardinal workers) do
    let i, m = Utils.recv router in
    if bar = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m;
  done;
  Hashtbl.fold (fun k v l -> v :: l) h []

(* Delay bounded parallel *)
let dbp bar router workers msgbuf =
  let h = Hashtbl.create 1024 in
  (* first check the buffer for those arrive early *)
  List.iter (fun (i,m) ->
    if not (Hashtbl.mem h i) then Hashtbl.(add h i m; remove msgbuf bar)
  ) (Hashtbl.find_all msgbuf bar);
  (* then wait for the rest of the messages *)
  let budget = 0.001 in
  let t0 = Unix.gettimeofday () in
  (try while (Hashtbl.length h) < (StrMap.cardinal workers) do
    let i, m = Utils.recv router in
    if bar = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m;
    if budget < (Unix.gettimeofday () -. t0) then failwith "timeout"
  done
  with exn -> Logger.info "%s" "timeout +++");
  Hashtbl.fold (fun k v l -> v :: l) h []

(* Stale synchronous parallel *)
let ssp = None

(* P2P barrier: Asynchronous parallel *)
let p2p_asp step step_buf wait_bar context updates = true

(* P2P barrier : Bulk synchronous parallel
   this one waits for the slowest one to catch up. *)
let p2p_bsp step step_buf wait_bar context updates =
  match wait_bar with
  | true  -> (
      List.for_all (fun k ->
        let t = Hashtbl.find step_buf k in
        t >= step
      ) (StrMap.keys context.workers)
    )
  | false -> false
