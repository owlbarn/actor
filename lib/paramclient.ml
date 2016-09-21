(** [ Parameter ]
  provides an interface to global varialbe like KV store
*)

open Types

let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _master : [`Dealer] ZMQ.Socket.t list ref = ref []
let _ps () = List.nth !_master 0

(** current step at client *)
let _step = ref 0

(** default push function *)
let _default_push = fun worker_id vars -> []
let _push = ref (Marshal.to_string _default_push [ Marshal.Closures ])

let get k =
  Logger.debug "get -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:!_step (_ps ()) PS_Get [|k'|];
  let m = of_msg (ZMQ.Socket.recv ~block:true (_ps ())) in
  Marshal.from_string m.par.(0) 0, m.bar

let set k v t =
  Logger.debug "set -> %s" _context.master;
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t (_ps ()) PS_Set [|k'; v'|]

let update_param x t =
  (** update multiple kvs, more efficient than set *)
  Logger.info "push -> %s" _context.master;
  let x' = Marshal.to_string x [] in
  Utils.send ~bar:t (_ps ()) PS_Push [|x'|]

let service_loop _addr _router =
  Logger.info "parameter worker @ %s" _addr;
  (** unmarshal the push function *)
  let push : ('a, 'b, 'c) ps_push_typ = Marshal.from_string !_push 0 in
  (** loop to process messages *)
  try while true do
    let i, m = Utils.recv _router in
    let t = m.bar in
    match m.typ with
    | PS_Schedule -> (
      Logger.info "scheduled @ %s" _addr;
      let _ = _step := if t > !_step then t else !_step in
      let vars = Marshal.from_string m.par.(0) 0 in
      let updates = push _addr vars in
      update_param updates t
      )
    | Terminate -> (
      Logger.info "%s" ("terminate @ " ^ _addr);
      Utils.send ~bar:t (_ps ()) OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ _context.jid ^ " terminated")
      )
    | _ -> ( Logger.debug "%s" "unknown mssage to PS" )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close _router;
    Pervasives.exit 0 )

let init m jid _addr _router _ztx =
  let _ = _context.jid <- jid; _context.master <- m.par.(0) in
  (* connect to job master *)
  let master = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark master Config.high_warter_mark;
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master m.par.(0);
  Utils.send master OK [|_addr|];
  _master := [master];
  (** enter into worker service loop *)
  service_loop _addr _router
