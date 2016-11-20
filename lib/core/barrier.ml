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
let p2p_asp step wait_bar context updates = true

(* P2P barrier : Bulk synchronous parallel *)
(* FIXME : not working atm *)
let p2p_bsp step wait_bar context updates =
  if wait_bar = false then false
  else (
    Logger.error "=====++++";
    StrMap.values context.workers
    |> List.for_all (fun s ->
      Utils.send s P2P_Bar_Q [|context.myself_addr|];
      let _, m = Utils.recv context.myself_sock in
      let t = Marshal.from_string m.par.(0) 0 in
      t = step
    )
  )
