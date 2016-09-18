(** [ Parameter ]
  provides an interface to global varialbe like KV store
*)

open Types

let _ztx = ZMQ.Context.create ()
let _ps : [`Dealer] ZMQ.Socket.t = ZMQ.Socket.(create _ztx dealer)
let _ = ZMQ.Socket.connect _ps Config.ps_addr
let _context = { jid = ""; master = ""; worker = StrMap.empty }

let _schedule = None
let _push = None
let _pull = None

let get k t =
  Logger.debug "GET -> %s" Config.ps_addr;
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:t _ps PS_Get [|k'|];
  ZMQ.Socket.recv ~block:true _ps

let set k v t =
  Logger.debug "SET -> %s" Config.ps_addr;
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t _ps PS_Set [|k'; v'|]

let worker_fun jid m _ztx _addr _router =
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master m.par.(0);
  Utils.send master OK [|_addr|];
  while true do
    Logger.debug "worker ... %s" jid;
    Unix.sleep 2;
  done
