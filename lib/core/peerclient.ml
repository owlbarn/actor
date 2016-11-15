(** [ Peer-to-Peer Parallel ] Client module *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_context ())

let service_loop () =
  Logger.debug "p2p_client @ %s" !_context.master_addr;
  (* loop to process messages *)
  try while true do
    Unix.sleep 5;
    Utils.send !_context.master_sock P2P_Forward [|!_context.master_addr; "hello"|];
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock;
    Pervasives.exit 0 )

let init m context =
  _context := context;
  !_context.ztx <- ZMQ.Context.create ();
  !_context.master_addr <- context.myself_addr;
  !_context.myself_addr <- "p2p_client";
  (* connect to local p2p server *)
  let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark sock Config.high_warter_mark;
  ZMQ.Socket.set_identity sock "p2p_client";
  ZMQ.Socket.connect sock !_context.master_addr;
  !_context.master_sock <- sock;
  (* enter into worker service loop *)
  service_loop ()
