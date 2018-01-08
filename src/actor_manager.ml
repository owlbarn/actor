(** [ Manager ]
  keeps running to manage a group of actors
*)

open Actor_types

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

let addr = Actor_config.manager_addr
let myid = Actor_config.manager_id

let process r m =
  match m.typ with
  | User_Reg -> (
    let uid, addr = m.par.(0), m.par.(1) in
    if Workers.mem uid = false then
      Actor_logger.info "%s" (uid ^ " @ " ^ addr);
      Workers.add uid addr;
      Actor_utils.send r OK [||];
    )
  | Job_Reg -> (
    let master, jid = m.par.(0), m.par.(1) in
    if Actor_service.mem jid = false then (
      Actor_service.add jid master;
      (* FIXME: currently send back all nodes as workers *)
      let addrs = Marshal.to_string (Workers.addrs ()) [] in
      Actor_utils.send r Job_Master [|addrs|] )
    else
      let master = (Actor_service.find jid).master in
      Actor_utils.send r Job_Worker [|master|]
    )
  | Heartbeat -> (
    Actor_logger.info "%s" ("heartbeat @ " ^ m.par.(0));
    Workers.add m.par.(0) m.par.(1);
    Actor_utils.send r OK [||];
    )
  | P2P_Reg -> (
    let addr, jid = m.par.(0), m.par.(1) in
    Actor_logger.info "p2p @ %s job:%s" addr jid;
    if Actor_service.mem jid = false then Actor_service.add jid "";
    let peers = Actor_service.choose_workers jid 10 in
    let peers = Marshal.to_string peers [] in
    Actor_service.add_worker jid addr;
    Actor_utils.send r OK [|peers|];
    )
  | _ -> (
    Actor_logger.error "unknown message type"
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
