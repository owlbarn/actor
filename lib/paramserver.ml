(** [ Parameter Server ]
  provides a global variable like KV store
*)

open Types

let _param : (Obj.t, Obj.t * int) Hashtbl.t = Hashtbl.create 1_000_000
let _context = { jid = ""; master = ""; worker = StrMap.empty }

(** record: whether busy; worker's current step; workers at a step *)
let worker_busy : (string, int) Hashtbl.t = Hashtbl.create 1_000_000
let worker_step : (string, int) Hashtbl.t = Hashtbl.create 1_000_000
let step_worker : (int, string) Hashtbl.t = Hashtbl.create 1_000_000
let _step = ref 0 (** actually represents the lowest barrier *)

(** default schedule function *)
let _default_schedule = fun workers -> [ ] (** TODO: fix scheduler ... *)
let _schedule = ref (Marshal.to_string _default_schedule [ Marshal.Closures ])

(** default pull function *)
let _default_pull = fun updates -> updates
let _pull = ref (Marshal.to_string _default_pull [ Marshal.Closures ])

(** bulk synchronous parallel *)
let bsp t =
  let num_finish = List.length (Hashtbl.find_all step_worker t) in
  let num_worker = StrMap.cardinal _context.worker in
  match num_finish = num_worker with
  | true  -> t + 1, (StrMap.keys _context.worker)
  | false -> t, []

(** stale synchronous parallel *)
let ssp t d =
  let num_finish = List.length (Hashtbl.find_all step_worker t) in
  let num_worker = StrMap.cardinal _context.worker in
  let t = match num_finish = num_worker with
    | true  -> t + 1
    | false -> t
  in
  let l = Hashtbl.fold (fun w t' l ->
    let busy = Hashtbl.find worker_busy w in
    match (busy = 0) && ((t' - t) < d) with
    | true  -> l @ [ w ]
    | false -> l
  ) worker_step []
  in (t, l)

let update_steps t w =
  let t' = Hashtbl.find worker_step w in
  match t > t' with
  | true  -> (
    Hashtbl.replace worker_busy w 0;
    Hashtbl.replace worker_step w t;
    Hashtbl.add step_worker t w )
  | false -> ()

let get k =
  let k' = Obj.repr k in
  let v, t = Hashtbl.find _param k' in
  Obj.obj v, t

let set k v t =
  let k' = Obj.repr k in
  let v' = Obj.repr v in
  match Hashtbl.mem _param k' with
  | true  -> Hashtbl.replace _param k' (v',t)
  | false -> Hashtbl.add _param k' (v',t)

let _broadcast_all t s =
  let bar = Random.int 536870912 in
  StrMap.iter (fun k v -> Utils.send ~bar v t s) _context.worker;
  bar

let terminate () =
  let _ = _broadcast_all Terminate [||] in
  Unix.sleep 1 (** FIXME: change to BSP *)

let service_loop _router =
  Logger.info "parameter server @ %s" _context.master;
  (** unmarshal the schedule and pull functions *)
  let schedule : ('a, 'b, 'c) ps_schedule_typ = Marshal.from_string !_schedule 0 in
  let pull : ('a, 'b, 'c) ps_pull_typ = Marshal.from_string !_pull 0 in
  (** loop to process messages *)
  try while true do
    (** synchronisation barrier check *)
    let t, passed = bsp !_step in _step := t;
    (** schecule the passed at every message arrival *)
    let tasks = schedule passed in
    List.iter (fun (worker, task) ->
      let w = StrMap.find worker _context.worker in
      let s = Marshal.to_string task [] in
      let t = Hashtbl.find worker_step worker + 1 in
      let _ = Hashtbl.replace worker_busy worker 1 in
      Utils.send ~bar:t w PS_Schedule [|s|]
    ) tasks;
    if List.length tasks > 0 then
      Logger.debug "schedule t:%i -> %i workers" !_step (List.length tasks);
    (** wait for another message arrival *)
    let i, m = Utils.recv _router in
    let t = m.bar in
    match m.typ with
    | PS_Get -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v, t' = get k in
      let s = to_msg t' OK [| Marshal.to_string v [] |] in
      ZMQ.Socket.send_all ~block:false _router [i;s];
      Logger.debug "get <- dt = %i, %s" (t - t') i
      )
    | PS_Set -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v = Marshal.from_string m.par.(1) 0 in
      let _ = set k v t in
      Logger.debug "set <- t:%i, %s" t i
      )
    | PS_Push -> (
      let updates = Marshal.from_string m.par.(0) 0 |> pull in
      List.iter (fun (k,v) -> set k v t) updates;
      update_steps t i;
      Logger.debug "push <- t:%i, %s" t i
      )
    | _ -> ( Logger.debug "%s" "unknown mssage to PS" )
  done with Failure e -> (
    Logger.warn "%s" e;
    terminate ();
    ZMQ.Socket.close _router )

let init m jid _addr _router _ztx =
  let _ = _context.jid <- jid; _context.master = _addr in
  (** contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.map (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    Utils.send req Job_Create [|_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (** wait until all the allocated actors register *)
  while (StrMap.cardinal _context.worker) < (List.length addrs) do
    let i, m = Utils.recv _router in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
    ZMQ.Socket.set_send_high_water_mark s Config.high_warter_mark;
    ZMQ.Socket.connect s m.par.(0);
    _context.worker <- (StrMap.add m.par.(0) s _context.worker);
  done;
  (** initialise the step <--> work tables *)
  StrMap.iter (fun k v ->
    Hashtbl.add worker_busy k 0;
    Hashtbl.add worker_step k 0;
    Hashtbl.add step_worker 0 k;
  ) _context.worker;
  (** enter into master service loop *)
  service_loop _router
