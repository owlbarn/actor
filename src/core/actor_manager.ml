(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(* Manager: keeps running to manage a group of actors *)


module Make
  (Net : Actor_net.Sig)
  = struct

  module Types = Actor_types.Make(Net)
  open Types

  module Service = Actor_service.Make(Net)


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
    let to_list () = StrMap.fold (fun _k v l -> l @ [v]) !_workers []
    let addrs () = StrMap.fold (fun _k v l -> l @ [v.addr]) !_workers []
  end

  let addr = Actor_config.manager_addr
  let myid = Actor_config.manager_id

  let process r msg =
    match msg with
    | User_Reg (uid, addr) -> (
      if Workers.mem uid = false then
        Owl_log.info "%s" (uid ^ " @ " ^ addr);
        Workers.add uid addr;
        (* Actor_utils.send r OK [||]; *)
      )
    | Job_Reg (master, jid) -> (
      if Service.mem jid = false then (
        Service.add jid master;
        (* FIXME: currently send back all nodes as workers *)
        let ____addrs = Marshal.to_string (Workers.addrs ()) [] in
        Net.send r "Job_Master [|addrs|]"
      )
      else
        let ____master = (Service.find jid).master in
        Net.send r "Job_Worker [|master|]"
      )
    | Heartbeat (uid, addr) -> (
      Owl_log.info "%s" ("heartbeat @ " ^ uid);
      Workers.add uid addr;
      Net.send r "OK [||]";
      )
    | P2P_Reg (addr, jid) -> (
      Owl_log.info "p2p @ %s job:%s" addr jid;
      if Service.mem jid = false then Service.add jid "";
      let peers = Service.choose_workers jid 10 in
      let ____peers = Marshal.to_string peers [] in
      Service.add_worker jid addr;
      Net.send r "OK [|peers|]";
      )
    | _ -> (
      Owl_log.error "unknown message type"
      )

  let run addr =
    Net.init ();
    let rep = Net.listen addr in
    while true do
      let m = of_msg (Net.recv rep) in
      process rep m;
    done;
    Net.close rep;
    Net.exit ()


end
