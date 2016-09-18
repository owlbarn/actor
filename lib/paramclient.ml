(** [ Parameter ]
  provides an interface to global varialbe like KV store
*)

open Types

let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _master : [`Dealer] ZMQ.Socket.t list ref = ref []
let _ps () = List.nth !_master 0
let _step = ref 0

let _push : (string -> string list -> unit) ref = ref (fun worker_id vars -> ())

let get k t =
  Logger.debug "GET -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:t (_ps ()) PS_Get [|k'|];
  ZMQ.Socket.recv ~block:true (_ps ())

let set k v t =
  Logger.debug "SET -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t (_ps ()) PS_Set [|k'; v'|]

let service_loop _addr _router =
  Logger.info "parameter worker @ %s" _addr;
  (** loop to process messages *)
  try while true do
    let i, m = Utils.recv _router in
    let t = m.bar in
    match m.typ with
    | PS_Schedule -> (
      Logger.info "scheduled @ %s" _addr;
      (** TODO: call _push *)
      )
    | Terminate -> (
      Logger.info "%s" ("terminate @ " ^ _addr);
      Utils.send ~bar:t (_ps ()) OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ _context.jid ^ " terminated")
      )
    | _ -> (
      Logger.debug "%s" "unknown mssage to PS";
      )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close _router;
    Pervasives.exit 0 )

let init m jid _addr _router _ztx =
  let _ = _context.jid <- jid; _context.master = m.par.(0) in
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master m.par.(0);
  Utils.send master OK [|_addr|];
  _master := [master];
  (** enter into worker service loop *)
  service_loop _addr _router
