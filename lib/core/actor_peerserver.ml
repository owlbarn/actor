(** [ Peer-to-Peer Parallel ] Server module *)

open Actor_types

(* the global context: master, worker, etc. *)
let _context = ref (Actor_utils.empty_peer_context ())
let _param : (Obj.t, Obj.t * int) Hashtbl.t = Hashtbl.create 1_000_000

(* buffer of the requests and replies of pulling model parameters *)
let _plbuf : (Obj.t, Obj.t option) Hashtbl.t = Hashtbl.create 1_000

(* default pull function *)
let _default_pull _ updates = updates
let _pull = ref (Marshal.to_string _default_pull [ Marshal.Closures ])

(* default barrier function *)
let _default_barrier = fun _ -> true
let _barrier = ref (Marshal.to_string _default_barrier [ Marshal.Closures ])

(* routing table module *)
module Route = struct

  (* FIXME: given ONLY 30-bit address space *)
  let _space = 2. ** 30. |> int_of_float

  let hash x = Hashtbl.hash x

  let distance x y = (x - y + _space) mod _space

  let add addr sock =
    if Hashtbl.mem !_context.spbuf addr = false then Hashtbl.add !_context.spbuf addr 0;
    !_context.workers <- (StrMap.add addr sock !_context.workers)

  let exists addr = StrMap.mem addr !_context.workers

  let connect addr =
    let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.set_send_high_water_mark sock Actor_config.high_warter_mark;
    ZMQ.Socket.set_identity sock !_context.myself_addr;
    ZMQ.Socket.connect sock addr;
    sock

  let furthest x =
    let d = ref min_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = distance (hash y) x in
      if d' > !d then ( d := d'; n := y )
    ) (StrMap.keys !_context.workers @ [!_context.myself_addr]);
    !n

  let furthest_exclude x l =
    let addrs = StrMap.keys !_context.workers @ [!_context.myself_addr]
      |> List.filter (fun x -> not (List.mem x l))
    in
    let d = ref min_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = distance (hash y) x in
      if d' > !d then ( d := d'; n := y )
    ) addrs;
    !n

  let nearest x =
    let d = ref max_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = distance (hash y) x in
      if d' < !d then ( d := d'; n := y )
    ) (StrMap.keys !_context.workers @ [!_context.myself_addr]);
    !n

  let nearest_exclude x l =
    let addrs = StrMap.keys !_context.workers @ [!_context.myself_addr]
      |> List.filter (fun x -> not (List.mem x l))
    in
    let d = ref max_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = distance (hash y) x in
      if d' < !d then ( d := d'; n := y )
    ) addrs;
    !n

  let forward nxt typ msg =
    let s = StrMap.find nxt !_context.workers in
    Actor_utils.send ~bar:!_context.step s typ msg

  let init_table addrs =
    (* contact initial random peers *)
    Array.iter (fun x ->
      let s = connect x in
      let _ = add x s in
      Actor_utils.send s P2P_Ping [|!_context.myself_addr|];
    ) addrs;
    (* contact nodes of fixed distance *)
    let myid = hash !_context.myself_addr in
    let _ = Array.init 30 (fun i ->
      let d = 2. ** (float_of_int i) |> int_of_float in
      let a = (myid + d) mod _space in
      let n = nearest_exclude a [!_context.myself_addr] in
      if String.length n <> 0 then
        let s = Marshal.to_string a [] in
        forward n P2P_Join [|!_context.myself_addr; s|]
    ) in ()

end

let _get k =
  let k' = Obj.repr k in
  let v, t = Hashtbl.find _param k' in
  Obj.obj v, t

let _set k v t =
  let k' = Obj.repr k in
  let v' = Obj.repr v in
  match Hashtbl.mem _param k' with
  | true  -> Hashtbl.replace _param k' (v',t)
  | false -> Hashtbl.add _param k' (v',t)

let _allocate_params x y =
  let x = Route.hash x in
  let y = Route.hash y in
  let l = ref [] in
  Hashtbl.iter (fun k v ->
    let h = Obj.obj k |> Route.hash in
    if (Route.distance y h) < (Route.distance x h) then l := !l @ [(k,v)]
  ) _param; !l

let _shall_deliver_pull () =
  let ready = ref true in
  Hashtbl.iter (fun k v ->
    match v with Some _ -> () | None -> ready := false
  ) _plbuf;
  if !ready = true then (
    let s = Hashtbl.fold (fun _ v l ->
      match v with
      | Some v -> let k,v,t = Obj.obj v in l @ [(k,v)]
      | None -> l
    ) _plbuf []
    in
    let s = Marshal.to_string s [] in
    Actor_utils.send !_context.master_sock OK [|s|];
    Hashtbl.reset _plbuf
  )

let _barrier_control barrier pull =
  let updates = List.map Obj.obj !_context.mpbuf in
  if barrier _context = true then (
    pull _context updates |> List.iter (fun (k,v,t) -> _set k v t);
    !_context.mpbuf <- [];
    if !_context.block = true then (
      Actor_utils.send ~bar:!_context.step !_context.master_sock OK [||];
      !_context.block <- false
    )
  )

let _update_step_buf addr step =
  if Hashtbl.mem !_context.spbuf addr = true then (
    let step = max step (Hashtbl.find !_context.spbuf addr) in
    Hashtbl.replace !_context.spbuf addr step
  )
  else Hashtbl.add !_context.spbuf addr step

let _notify_peers_step () =
  List.iter (fun k ->
    Route.forward k P2P_Ping [|!_context.myself_addr|]
  ) (StrMap.keys !_context.workers)

let _process_timeout () =
  _notify_peers_step ();
  Actor_logger.debug "%s: timeout" !_context.myself_addr

let service_loop () =
  Actor_logger.debug "%s: p2p server" !_context.myself_addr;
  let barrier : p2p_barrier_typ = Marshal.from_string !_barrier 0 in
  let pull : ('a, 'b) p2p_pull_typ = Marshal.from_string !_pull 0 in
  (* loop to process messages *)
  ZMQ.Socket.set_receive_timeout !_context.myself_sock (1 * 1000);
  try while true do
    (* first, wait and process arriving message *)
    try let i, m = Actor_utils.recv !_context.myself_sock in (
    match m.typ with
    | P2P_Connect -> (
      Actor_logger.debug "%s: p2p_connect %s" !_context.myself_addr m.par.(0);
      let addr = m.par.(0) in
      !_context.master_addr <- addr;
      !_context.master_sock <- Route.connect addr
      )
    | P2P_Ping -> (
      Actor_logger.debug "%s: p2p_ping %s" !_context.myself_addr m.par.(0);
      let addr = m.par.(0) in
      if Route.exists addr = false then Route.(connect addr |> add addr)
      )
    | P2P_Join -> (
      Actor_logger.debug "%s: p2p_join %s" !_context.myself_addr m.par.(0);
      let src = m.par.(0) in
      let dst = Marshal.from_string m.par.(1) 0 in
      let next = Route.nearest_exclude dst [src] in
      if next = !_context.myself_addr then (
        if Route.exists src = false then (
          let s = Route.connect src in
          let _ = Route.add src s in
          Actor_utils.send s P2P_Ping [|!_context.myself_addr|]
        );
        (* oh, hello neighbour, maybe take my model *)
        if Route.hash src = dst - 1 then (
          let next = Route.furthest_exclude dst [src; !_context.myself_addr] in
          if String.length next <> 0 then
            Route.forward next P2P_Ping [|src|];
          let h = _allocate_params !_context.myself_addr src in
          let s = Marshal.to_string h [] in
          Actor_logger.debug "params: %s ===> %s size:%i" !_context.myself_addr src (String.length s);
          Route.forward src P2P_Copy [|s|]
        )
      );
      if next <> !_context.myself_addr && String.length next <> 0 then
        Route.forward next P2P_Join m.par;
      )
    | P2P_Copy -> (
      Actor_logger.debug "%s: p2p_copy" !_context.myself_addr;
      let h = Marshal.from_string m.par.(0) 0 in
      List.iter (fun (k,v) ->
        match Hashtbl.mem _param k with
        | true  -> Hashtbl.replace _param k v
        | false -> Hashtbl.add _param k v
      ) h
      )
    | P2P_Get -> (
      Actor_logger.debug "%s: p2p_get" !_context.myself_addr;
      let k = Marshal.from_string m.par.(0) 0 in
      let next = Route.(hash k |> nearest) in
      match next = !_context.myself_addr with
      | true  -> (
          (* FIXME: what if i cannot find the k *)
          let v, t = _get k in
          let s = Marshal.to_string (k, v, t) [] in
          Actor_utils.send !_context.master_sock OK [|s; next|]
        )
      | false -> Route.forward next P2P_Get_Q m.par
      )
    | P2P_Get_Q -> (
      Actor_logger.debug "%s: p2p_get_q" !_context.myself_addr;
      let k = Marshal.from_string m.par.(0) 0 in
      let next = Route.(hash k |> nearest) in
      match next = !_context.myself_addr with
      | true  -> (
          let v, t = _get k in
          let s = Marshal.to_string (k, v, t) [] in
          let addr = m.par.(1) in
          let next = Route.(hash addr |> nearest) in
          Route.forward next P2P_Get_R [|s; addr|]
        )
      | false -> Route.forward next P2P_Get_Q m.par
      )
    | P2P_Get_R -> (
      Actor_logger.debug "%s: p2p_get_r" !_context.myself_addr;
      let addr = m.par.(1) in
      let next = Route.(hash addr |> nearest) in
      match next = !_context.myself_addr with
      | true  -> Actor_utils.send !_context.master_sock OK m.par
      | false -> Route.forward next P2P_Get_R m.par
      )
    | P2P_Set -> (
      Actor_logger.debug "%s: p2p_get" !_context.myself_addr;
      let k, v, t = Marshal.from_string m.par.(0) 0 in
      (* check whether this is from the local client *)
      let t = if t < 0 then (
        let s = Marshal.to_string (k, v, !_context.step) [] in
        m.par <- [|s|]; !_context.step
      ) else t
      in
      let next = Route.(hash k |> nearest) in
      match next = !_context.myself_addr with
      | true  -> !_context.mpbuf <- !_context.mpbuf @ [Obj.repr (k, v, t)]
      | false -> Route.forward next P2P_Set m.par
      )
    | P2P_Push -> (
      Actor_logger.debug "%s: p2p_push" !_context.myself_addr;
      Marshal.from_string m.par.(0) 0
      |> List.iter (fun (k,v) ->
        let next = Route.(hash k |> nearest) in
        match next = !_context.myself_addr with
        | true  -> !_context.mpbuf <- !_context.mpbuf @ [Obj.repr (k, v, !_context.step)]
        | false -> (
            let s = Marshal.to_string (k, v, !_context.step) [] in
            Route.forward next P2P_Set [|s|]
          )
      )
      )
    | P2P_Pull -> (
      Actor_logger.debug "%s: p2p_pull" !_context.myself_addr;
      Marshal.from_string m.par.(0) 0
      |> List.iter (fun k ->
        let next = Route.(hash k |> nearest) in
        match next = !_context.myself_addr with
        | true  -> (
            let v, t = _get k in
            Hashtbl.add _plbuf (Obj.repr k) (Some (Obj.repr (k,v,t)))
          )
        | false -> (
            let y = Marshal.to_string k [] in
            let s = [|y; !_context.myself_addr|] in
            Route.forward next P2P_Pull_Q s;
            Hashtbl.add _plbuf (Obj.repr k) None
          )
      );
      _shall_deliver_pull ()
      )
    | P2P_Pull_Q -> (
      Actor_logger.debug "%s: p2p_pull_q %s" !_context.myself_addr m.par.(1);
      let k = Marshal.from_string m.par.(0) 0 in
      let next = Route.(hash k |> nearest) in
      match next = !_context.myself_addr with
      | true  -> (
          let v, t = _get k in
          let s = Marshal.to_string (k, v, t) [] in
          let addr = m.par.(1) in
          let next = Route.(hash addr |> nearest) in
          Route.forward next P2P_Pull_R [|s; addr|]
        )
      | false -> Route.forward next P2P_Pull_Q m.par
      )
    | P2P_Pull_R -> (
      Actor_logger.debug "%s: p2p_pull_r %s" !_context.myself_addr m.par.(1);
      let addr = m.par.(1) in
      let next = Route.(hash addr |> nearest) in
      match next = !_context.myself_addr with
      | true  -> (
          let k, v, t = Marshal.from_string m.par.(0) 0 in
          Hashtbl.replace _plbuf (Obj.repr k) (Some (Obj.repr (k,v,t)));
          _shall_deliver_pull ()
      )
      | false -> Route.forward next P2P_Pull_R m.par
      )
    | P2P_Bar -> (
      Actor_logger.debug "%s: p2p_bar" !_context.myself_addr;
      !_context.block <- true;
      !_context.step <- !_context.step + 1;
      _notify_peers_step ();
      )
    | _ -> ( Actor_logger.error "unknown mssage type" ) );
    (* second, update the piggybacked step  *)
    if i <> !_context.master_addr then _update_step_buf i m.bar;
    (* third, check the barrier control *)
    _barrier_control barrier pull;
    (* fourth, in case the process hangs *)
    with Unix.Unix_error (_,_,_) -> _process_timeout ()
  done with Failure e -> (
    Actor_logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock )

let init m context =
  _context := context;
  (* contact allocated peers to join the swarm *)
  Marshal.from_string m.par.(0) 0 |> Route.init_table;
  (* enter into server service loop *)
  service_loop ()
