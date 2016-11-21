(** [ Model Parallel ] Parameter client module  *)

open Types

(* the global context: master, worker, etc. *)
let _context = ref (Utils.empty_param_context ())

(* default push function *)
let _default_push = fun worker_id vars -> []
let _push = ref (Marshal.to_string _default_push [ Marshal.Closures ])

let _get k =
  Logger.debug "get -> %s" !_context.master_addr;
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:!_context.step !_context.master_sock PS_Get [|k'|];
  let m = of_msg (ZMQ.Socket.recv ~block:true !_context.master_sock) in
  Marshal.from_string m.par.(0) 0, m.bar

let _set k v t =
  Logger.debug "set -> %s" !_context.master_addr;
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t !_context.master_sock PS_Set [|k'; v'|]

let update_param x t =
  (* update multiple kvs, more efficient than set *)
  Logger.debug "push -> %s" !_context.master_addr;
  let x' = Marshal.to_string x [] in
  Utils.send ~bar:t !_context.master_sock PS_Push [|x'|]

let service_loop () =
  Logger.debug "parameter worker @ %s" !_context.myself_addr;
  (* unmarshal the push function *)
  let push : ('a, 'b, 'c) ps_push_typ = Marshal.from_string !_push 0 in
  (* loop to process messages *)
  try while true do
    let i, m = Utils.recv !_context.myself_sock in
    let t = m.bar in
    match m.typ with
    | PS_Schedule -> (
      Logger.debug "scheduled @ %s" !_context.myself_addr;
      !_context.step <- (if t > !_context.step then t else !_context.step);
      let vars = Marshal.from_string m.par.(0) 0 in
      let updates = push !_context.myself_addr vars in
      update_param updates t
      )
    | Terminate -> (
      Logger.debug "%s" ("terminate @ " ^ !_context.myself_addr);
      Utils.send ~bar:t !_context.master_sock OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ !_context.job_id ^ " terminated")
      )
    | _ -> ( Logger.debug "%s" "unknown mssage to PS" )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock;
    Pervasives.exit 0 )

let init m context =
  _context := context;
  !_context.master_addr <- m.par.(0);
  (* connect to job master *)
  let master = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark master Config.high_warter_mark;
  ZMQ.Socket.set_identity master !_context.myself_addr;
  ZMQ.Socket.connect master !_context.master_addr;
  Utils.send master OK [|!_context.myself_addr|];
  !_context.master_sock <- master;
  (* enter into worker service loop *)
  service_loop ()
