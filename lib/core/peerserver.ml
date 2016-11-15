(** [ Peer-to-Peer Parallel ] Server module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())

(* routing table module *)
module Route = struct

  let _client : [`Dealer] ZMQ.Socket.t ref =
    ref (ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer)

  let add addr sock = !_context.workers <- (StrMap.add addr sock !_context.workers)

  let exists addr = StrMap.mem addr !_context.workers

  let connect addr =
    let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.set_send_high_water_mark sock Config.high_warter_mark;
    ZMQ.Socket.connect sock addr;
    sock

  let next_hop x =
    let x = Hashtbl.hash x in
    let d = ref max_int in
    let n = ref "" in
    List.iteri (fun i y ->
      let d' = Hashtbl.hash y - x |> abs in
      if d' < !d then ( d := d'; n := y )
    ) (StrMap.keys !_context.workers @ [!_context.myself_addr]);
    !n

  let forward n msg =
    let s = StrMap.find n !_context.workers in
    Utils.send s P2P_Forward msg

end

let service_loop () =
  Logger.debug "p2p server @ %s" !_context.myself_addr;
  (* loop to process messages *)
  try while true do
    (** wait for another message arrival *)
    let i, m = Utils.recv !_context.myself_sock in
    (* let t = m.bar in *)
    match m.typ with
    | P2P_Ping -> (
      Logger.debug "%s pings" m.par.(0);
      let addr = m.par.(0) in
      if Route.exists addr = false then Route.(connect addr |> add addr)
      )
    | P2P_Forward -> (
      let addr = m.par.(0) in
      let next = Route.next_hop addr in
      if next = !_context.myself_addr then ( Logger.debug "%s -> %s" addr next )
      else Route.forward next [|addr; m.par.(1)|]
      )
    | P2P_Connect -> (
      let addr = m.par.(0) in
      Route.(_client := connect addr)
      )
    | P2P_Get -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v = Marshal.to_string (k ^ k) [] in
      Utils.send Route.(!_client) OK [|v|]
      )
    | _ -> ( Logger.error "unknown mssage type" )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock )

let init m context =
  _context := context;
  (* contact allocated peers to join the swarm *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  Array.iter (fun x ->
    let s = Route.connect x in
    let _ = Route.add x s in
    Utils.send s P2P_Ping [|!_context.myself_addr|];
  ) addrs;
  (* enter into master service loop *)
  service_loop ()
