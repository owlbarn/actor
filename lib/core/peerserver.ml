(** [ Peer-to-Peer Parallel ] Server module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())

(* routing table module *)
module Route = struct

  let _range = ref (0, 0)

  let add addr sock = !_context.workers <- (StrMap.add addr sock !_context.workers)

  let connect addr =
    let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
    ZMQ.Socket.set_send_high_water_mark sock Config.high_warter_mark;
    ZMQ.Socket.connect sock addr;
    sock

  let exists addr = StrMap.mem addr !_context.workers

  let forward msg = ()

  let is_mine msg = ()

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
