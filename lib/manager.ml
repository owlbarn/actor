(** [ Manager ]
  keeps running to manage a group of actors
*)

open Types

module Workers = struct
  let _workers = ref StrMap.empty

  let create id addr = {
    id = id;
    addr = addr;
    last_seen = Unix.time ()
  }

  let add id addr = _workers := StrMap.add id (create id addr) !_workers
  let remove id = _workers := StrMap.remove id !_workers
  let mem id = StrMap.mem id !_workers
  let to_list () = StrMap.fold (fun k v l -> l @ [v]) !_workers []
  let addrs () = StrMap.fold (fun k v l -> l @ [v.addr]) !_workers []
end

let addr = Config.manager_addr
let myid = Config.manager_id

let process r m =
  match m.typ with
  | User_Reg -> (
    let uid, addr = m.par.(0), m.par.(1) in
    if Workers.mem uid = false then
      Logger.info "%s" (uid ^ " @ " ^ addr);
      Workers.add uid addr;
      Utils.send r OK [||];
    )
  | Job_Reg -> (
    let master, jid = m.par.(0), m.par.(1) in
    if Service.mem jid = false then (
      Service.add jid master;
      (* FIXME: currently send back all nodes as workers *)
      let addrs = Marshal.to_string (Workers.addrs ()) [] in
      Utils.send r Job_Master [|addrs|] )
    else
      let master = (Service.find jid).master in
      Utils.send r Job_Worker [|master|]
    )
  | Heartbeat -> (
    Logger.info "%s" ("heartbeat @ " ^ m.par.(0));
    Workers.add m.par.(0) m.par.(1);
    Utils.send r OK [||];
    )
  | P2P_Reg -> (
    let addr, jid = m.par.(0), m.par.(1) in
    Logger.info "P2P @ %s Job:%s" addr jid;
    Utils.send r OK [||];
    )
  | _ -> (
    Logger.error "unrecognised message type"
    )

let run id addr =
  let _ztx = ZMQ.Context.create () in
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv rep) in
    process rep m;
  done;
  ZMQ.Socket.close rep;
  ZMQ.Context.terminate _ztx

let install_app x = None

let _ = run myid addr
