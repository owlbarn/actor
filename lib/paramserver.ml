(** [ Parameter Server ]
  provides a global variable like KV store
*)

open Types

let _param : (Obj.t, Obj.t * int) Hashtbl.t = Hashtbl.create 1_000_000
let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _step = ref 0

let get k =
  let k' = Obj.repr k in
  let v, t' = Hashtbl.find _param k' in
  v, t'

let set k v t =
  let k' = Obj.repr k in
  match Hashtbl.mem _param k with
  | true -> Hashtbl.replace _param k' (v,t)
  | false -> Hashtbl.add _param k' (v,t)

let service_loop _router =
  Logger.info "%s" "parameter server starts ...";
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
      Logger.debug "GET dt = %i @ %s" (t - t') _context.master
      )
    | PS_Set -> (
      let k = Marshal.from_string m.par.(0) 0 in
      let v = Marshal.from_string m.par.(1) 0 in
      let _ = set k v t in
      Logger.debug "SET t:%i @ %s" t _context.master
      )
    | _ -> (
      Logger.debug "%s" "unknown mssage to PS";
      )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.close _router
  )

let master_init m jid _addr _router _ztx =
  let _ = _context.jid <- jid; _context.master = _addr in
  (** contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.map (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    Utils.send req Job_Create [|_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (** wait until all the allocated actors register *)
  while (StrMap.cardinal _context.worker) < (List.length addrs) do
    let i, m = Utils.recv _router in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m.par.(0);
    _context.worker <- (StrMap.add m.par.(0) s _context.worker);
  done;
  (** enter into master service loop *)
  service_loop _router
