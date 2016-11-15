(** [ Peer-to-Peer Parallel ] Client module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())

let _get k =
  let k = Marshal.to_string k [] in
  Utils.send !_context.master_sock P2P_Get [|k|];
  let _, m = Utils.recv !_context.myself_sock in
  Marshal.from_string m.par.(0) 0

let set k v = None

let service_loop () =
  Logger.debug "p2p_client @ %s" !_context.master_addr;
  (* loop to process messages *)
  try while true do
    Unix.sleep 5;
    let v = _get "abc " in
    Logger.debug "get : %s" v
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock;
    Pervasives.exit 0 )

let init m context =
  _context := context;
  (* re-initialise since it is a new process *)
  !_context.ztx <- ZMQ.Context.create ();
  !_context.master_addr <- context.myself_addr;
  let _addr, _router = Utils.bind_available_addr !_context.ztx in
  !_context.myself_addr <- _addr;
  !_context.myself_sock <- _router;
  (* set up local p2p server <-> client *)
  let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark sock Config.high_warter_mark;
  ZMQ.Socket.set_identity sock !_context.myself_addr;
  ZMQ.Socket.connect sock !_context.master_addr;
  !_context.master_sock <- sock;
  Utils.send !_context.master_sock P2P_Connect [|!_context.myself_addr|];
  (* enter into worker service loop *)
  service_loop ()
