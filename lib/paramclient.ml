(** [ Parameter ]
  provides an interface to global varialbe like KV store
*)

open Types

let _ps : [`Dealer] ZMQ.Socket.t list ref = ref []
let _context = { jid = ""; master = ""; worker = StrMap.empty }

let _schedule = None
let _push = None
let _pull = None

let get k t =
  Logger.debug "GET -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:t (List.nth !_ps 0) PS_Get [|k'|];
  ZMQ.Socket.recv ~block:true (List.nth !_ps 0)

let set k v t =
  Logger.debug "SET -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t (List.nth !_ps 0) PS_Set [|k'; v'|]

let service_loop _router =
  while true do
    Unix.sleep 2;
    Logger.debug "worker ... %s" _context.jid
  done

let worker_init m jid _addr _router _ztx =
  let _ = _context.jid <- jid; _context.master = m.par.(0) in
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master m.par.(0);
  Utils.send master OK [|_addr|];
  _ps := [master];
  (** enter into worker service loop *)
  service_loop _router
