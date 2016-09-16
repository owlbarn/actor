(** [ Parameter Server ]
  provides a global variable like KV store
*)

open Types

let _param : (Obj.t, Obj.t * int) Hashtbl.t = Hashtbl.create 1_000_000
let _ztx = ZMQ.Context.create ()

let get k =
  let k' = Obj.repr k in
  let v, t' = Hashtbl.find _param k' in
  v, t'

let set k v t =
  let k' = Obj.repr k in
  match Hashtbl.mem _param k with
  | true -> Hashtbl.replace _param k' (v,t)
  | false -> Hashtbl.add _param k' (v,t)

let start () =
  Logger.info "%s" "parameter server starts ...";
  let _router = ZMQ.Socket.(create _ztx router) in
  ZMQ.Socket.bind _router Config.ps_addr;
  ZMQ.Socket.set_receive_high_water_mark _router Config.high_warter_mark;
  (** loop to process messages *)
  try while true do
    let i, m = Utils.recv _router in
    let t = m.bar in
    match m.typ with
    | PS_Get -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v, t' = get k in
      let s = to_msg t OK [| Marshal.to_string v [] |] in
      ZMQ.Socket.send_all ~block:false _router [i;s];
      Logger.debug "GET dt = %i @ %s" (t - t') Config.ps_addr
      )
    | PS_Set -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v = Marshal.from_string m.par.(1) 0 in
      let _ = set k v t in
      Logger.debug "SET t:%i @ %s" t Config.ps_addr
      )
    | _ -> (
      Logger.debug "%s" "unknown mssage to PS";
      )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close _router;
    Pervasives.exit 0 )


(** start parameter server *)

let _ = start ()
