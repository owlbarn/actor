(** [ Barrier module ]
  provides flexible synchronisation barrier controls.
*)

open Actor_types

(* Mapre barrier: Bulk synchronous parallel *)
let mapre_bsp bar _context =
  let h = Hashtbl.create 1024 in
  (* first check the buffer for those arrive early *)
  List.iter (fun (i,m) ->
    if not (Hashtbl.mem h i) then Hashtbl.(add h i m; remove !_context.msbuf bar)
  ) (Hashtbl.find_all !_context.msbuf bar);
  (* then wait for the rest of the messages *)
  while (Hashtbl.length h) < (StrMap.cardinal !_context.workers) do
    let i, m = Actor_utils.recv !_context.myself_sock in
    if bar = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m;
  done;
  Hashtbl.fold (fun k v l -> v :: l) h []

(* Mapre barrier: Delay bounded parallel *)
let mapre_dbp bar _context =
  let h = Hashtbl.create 1024 in
  (* first check the buffer for those arrive early *)
  List.iter (fun (i,m) ->
    if not (Hashtbl.mem h i) then Hashtbl.(add h i m; remove !_context.msbuf bar)
  ) (Hashtbl.find_all !_context.msbuf bar);
  (* then wait for the rest of the messages *)
  let budget = 0.001 in
  let t0 = Unix.gettimeofday () in
  (try while (Hashtbl.length h) < (StrMap.cardinal !_context.workers) do
    let i, m = Actor_utils.recv !_context.myself_sock in
    if bar = m.bar && not (Hashtbl.mem h i) then Hashtbl.add h i m;
    if budget < (Unix.gettimeofday () -. t0) then failwith "timeout"
  done
  with exn -> Actor_logger.info "%s" "timeout +++");
  Hashtbl.fold (fun k v l -> v :: l) h []

(* Param barrier: Bulk synchronous parallel *)
let param_bsp _context =
  let num_finish = List.length (Hashtbl.find_all !_context.step_worker !_context.step) in
  let num_worker = StrMap.cardinal !_context.workers in
  match num_finish = num_worker with
  | true  -> !_context.step + 1, (StrMap.keys !_context.workers)
  | false -> !_context.step, []

(* Param barrier: Stale synchronous parallel *)
let param_ssp _context =
  let num_finish = List.length (Hashtbl.find_all !_context.step_worker !_context.step) in
  let num_worker = StrMap.cardinal !_context.workers in
  let t = match num_finish = num_worker with
    | true  -> !_context.step + 1
    | false -> !_context.step
  in
  let l = Hashtbl.fold (fun w t' l ->
    let busy = Hashtbl.find !_context.worker_busy w in
    match (busy = 0) && ((t' - t) < !_context.stale) with
    | true  -> l @ [ w ]
    | false -> l
  ) !_context.worker_step []
  in (t, l)

(* Param barrier: Asynchronous parallel *)
let param_asp _context = !_context.stale <- max_int; param_ssp _context

(* P2P barrier : Bulk synchronous parallel
   this one waits for the slowest one to catch up. *)
let p2p_bsp _context =
  match !_context.block with
  | true  -> (
      List.for_all (fun k ->
        let t = Hashtbl.find !_context.spbuf k in
        t >= !_context.step
      ) (StrMap.keys !_context.workers)
    )
  | false -> false

(* P2P barrier: Asynchronous parallel *)
let p2p_asp _context = true

(* P2P barrier: Asynchronous parallel but aligned with local client *)
let p2p_asp_local _context = !_context.block
