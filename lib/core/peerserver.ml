(** [ Peer-to-Peer Parallel ] Server module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())
let _param : (Obj.t, Obj.t * int) Hashtbl.t = Hashtbl.create 1_000_000
let _pmbuf = ref []   (* buffer of the model updates *)
let _step = ref 0     (* local step for barrier control *)

(* default pull function *)
let _default_pull = List.map (fun o -> let _, k, v, t = Obj.obj o in k, v, t)
let _pull = ref (Marshal.to_string _default_pull [ Marshal.Closures ])

(* default barrier function *)
let _default_barrier = fun _ -> true
let _barrier = ref (Marshal.to_string _default_barrier [ Marshal.Closures ])

(* routing table module *)
module Route = struct

  (* FIXME: given ONLY 30-bit address space *)
  let _space = 2. ** 30. |> int_of_float

  let _client : [`Dealer] ZMQ.Socket.t ref =
    ref (ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer)

  let hash x = Hashtbl.hash x

  let distance x y = (x - y + _space) mod _space

  let add addr sock = !_context.workers <- (StrMap.add addr sock !_context.workers)

  let exists addr = StrMap.mem addr !_context.workers

  let connect addr =
    let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.set_send_high_water_mark sock Config.high_warter_mark;
    ZMQ.Socket.connect sock addr;
    sock

  let next_hop x =
    let d = ref max_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = distance (hash y) x in
      if d' < !d then ( d := d'; n := y )
    ) (StrMap.keys !_context.workers @ [!_context.myself_addr]);
    !n

  let next_hop_exclude x l =
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
    Utils.send s typ msg

  let init_table addrs =
    (* contact initial random peers *)
    Array.iter (fun x ->
      let s = connect x in
      let _ = add x s in
      Utils.send s P2P_Ping [|!_context.myself_addr|];
    ) addrs;
    let myid = hash !_context.myself_addr in
    let m = (2. ** 30.) |> int_of_float in
    let _ = Array.init 30 (fun i ->
      let d = 2. ** (float_of_int i) |> int_of_float in
      let a = (d + myid) mod m in
      let n = next_hop_exclude a [!_context.myself_addr] in
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

let _allocate_params k = ()

let service_loop () =
  Logger.debug "p2p server @ %s" !_context.myself_addr;
  let barrier : 'a list -> bool = Marshal.from_string !_barrier 0 in
  let pull : 'a list -> 'b list = Marshal.from_string !_pull 0 in
  (* loop to process messages *)
  try while true do
    (* barrier control *)
    if barrier !_pmbuf = true then (
      pull !_pmbuf |> List.iter (fun (k,v,t) -> _set k v t);
      _step := !_step + 1;
      _pmbuf := [];
    );
    (* wait for another message arrival *)
    let i, m = Utils.recv !_context.myself_sock in
    (* let t = m.bar in *)
    match m.typ with
    | P2P_Ping -> (
      Logger.debug "%s pings" m.par.(0);
      let addr = m.par.(0) in
      if Route.exists addr = false then Route.(connect addr |> add addr)
      )
    | P2P_Join -> (
      let src = m.par.(0) in
      let dst = Marshal.from_string m.par.(1) 0 in
      let next = Route.next_hop_exclude dst [src] in
      if next = !_context.myself_addr then (
        if Route.exists src = false then (
          let s = Route.connect src in
          let _ = Route.add src s in
          Utils.send s P2P_Ping [|!_context.myself_addr|]
        );
        (* oh, hello neighbour, take my model *)
        if (Route.hash src) = (dst - 1) then (
          Logger.error "oh neighbour: %s <-> %s" next src;
          _allocate_params (Route.hash src);
        )
        (* TODO: insert node ... *)
      );
      if next <> !_context.myself_addr && String.length next <> 0 then
        Route.forward next P2P_Join m.par;
      )
    | P2P_Connect -> (
      Logger.debug "%s connects" m.par.(0);
      let addr = m.par.(0) in
      Route.(_client := connect addr)
      )
    | P2P_Forward -> (
      Logger.debug "forward to %s" m.par.(0);
      let addr = m.par.(0) in
      let next = Route.(hash addr |> next_hop) in
      match next = !_context.myself_addr with
      | true  -> Utils.send Route.(!_client) OK m.par
      | false -> Route.forward next P2P_Forward m.par
      )
    | P2P_Get -> (
      Logger.debug "client get @ %s" !_context.myself_addr;
      let k = Marshal.from_string m.par.(0) 0 in
      let next = Route.(hash k |> next_hop) in
      match next = !_context.myself_addr with
      | true  -> (
          (* FIXME: what if i cannot find the k *)
          let v, t = _get k in
          let s = Marshal.to_string (k, v, t) [] in
          (* check whether this is from local client *)
          match m.par.(1) = !_context.myself_addr with
          | true  -> Utils.send Route.(!_client) OK [|next; s|]
          | false -> (
              let addr = m.par.(1) in
              let next = Route.(hash addr |> next_hop) in
              Route.forward next P2P_Forward [|addr; s|]
            )
        )
      | false -> Route.forward next P2P_Get m.par
      )
    | P2P_Set -> (
      Logger.debug "set @ %s" !_context.myself_addr;
      let k, v, t = Marshal.from_string m.par.(0) 0 in
      (* check whether this is from local client *)
      let t = if t < 0 then (
        let s = Marshal.to_string (k, v, !_step) [] in
        m.par <- [|s|]; !_step
      ) else t
      in
      let next = Route.(hash k |> next_hop) in
      match next = !_context.myself_addr with
      | true  -> _pmbuf := !_pmbuf @ [Obj.repr (i, k, v, t)]
      | false -> Route.forward next P2P_Set m.par
      )
    | _ -> ( Logger.error "unknown mssage type" )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock )

let init m context =
  _context := context;
  (* contact allocated peers to join the swarm *)
  Marshal.from_string m.par.(0) 0 |> Route.init_table;
  (* enter into server service loop *)
  service_loop ()
