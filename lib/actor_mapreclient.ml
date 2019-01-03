(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

(** Data Parallel: Map-Reduce client module *)

open Actor_types


(* the global context: master, worker, etc. *)
let _context = ref (Actor_utils.empty_mapre_context ())


let barrier bar = Actor_barrier.mapre_bsp bar _context


let shuffle bar x z =
  List.mapi (fun i k ->
    let v = Actor_utils.choose_load x (List.length z) i in
    let s = if StrMap.mem k !_context.workers then
      StrMap.find k !_context.workers
    else (
      let s = Zmq.Socket.(create !_context.ztx dealer) in
      let _ = Zmq.Socket.(set_identity s !_context.myself_addr; connect s k) in
      let _ = !_context.workers <- StrMap.add k s !_context.workers in
      let _ = Zmq.Socket.set_send_high_water_mark s Actor_config.high_warter_mark in
      s ) in
    Actor_utils.send ~bar s OK [|Marshal.to_string v []|]
  ) z


let process_pipeline s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
        Owl_log.info "%s" ("map @ " ^ !_context.myself_addr);
        let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
        List.map f (Actor_memory.find m.par.(1)) |> Actor_memory.add m.par.(2)
      )
    | MapPartTask -> (
        Owl_log.info "%s" ("map_partition @ " ^ !_context.myself_addr);
        let f : 'a list -> 'b list = Marshal.from_string m.par.(0) 0 in
        f (Actor_memory.find m.par.(1)) |> Actor_memory.add m.par.(2)
      )
    | FilterTask -> (
        Owl_log.info "%s" ("filter @ " ^ !_context.myself_addr);
        let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
        List.filter f (Actor_memory.find m.par.(1)) |> Actor_memory.add m.par.(2)
      )
    | FlattenTask -> (
        Owl_log.info "%s" ("flatten @ " ^ !_context.myself_addr);
        List.flatten (Actor_memory.find m.par.(0)) |> Actor_memory.add m.par.(1)
      )
    | UnionTask -> (
        Owl_log.info "%s" ("union @ " ^ !_context.myself_addr);
        (Actor_memory.find m.par.(0)) @ (Actor_memory.find m.par.(1))
        |> Actor_memory.add m.par.(2)
      )
    | ReduceByKeyTask -> (
        Owl_log.info "%s" ("reduce_by_key @ " ^ !_context.myself_addr);
        let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
        Actor_memory.find m.par.(1) |> Actor_utils.group_by_key |> List.map (fun (k,l) ->
          match l with
          | hd :: tl -> (k, List.fold_left f hd tl)
          | [] -> failwith "error in reduce"
        )
        |> Actor_memory.add m.par.(2)
      )
    | JoinTask -> (
        Owl_log.info "%s" ("join @ " ^ !_context.myself_addr);
        (Actor_memory.find m.par.(0)) @ (Actor_memory.find m.par.(1))
        |> Actor_utils.group_by_key |> Actor_memory.add m.par.(2)
      )
    | ShuffleTask -> (
        Owl_log.info "%s" ("shuffle @ " ^ !_context.myself_addr);
        let x = Actor_memory.find m.par.(0) |> Actor_utils.group_by_key in
        let z = Marshal.from_string m.par.(2) 0 in
        let bar = Marshal.from_string m.par.(3) 0 in
        let _ = shuffle bar x z in
        barrier bar
        |> List.map (fun m -> Marshal.from_string m.par.(0) 0 |> Actor_utils.flatten_kvg)
        |> List.flatten |> Actor_memory.add m.par.(1);
      )
    | _ -> Owl_log.info "%s" "unknown task types"
  ) s


let service_loop () =
  Owl_log.debug "mapre worker @ %s" !_context.myself_addr;
  (* set up local loop of a job worker *)
  try while true do
    let i, m = Actor_utils.recv !_context.myself_sock in
    let bar = m.bar in
    match m.typ with
    | Count -> (
        Owl_log.info "%s" ("count @ " ^ !_context.myself_addr);
        let y = List.length (Actor_memory.find m.par.(0)) in
        Actor_utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|]
      )
    | Collect -> (
        Owl_log.info "%s" ("collect @ " ^ !_context.myself_addr);
        let y = Actor_memory.find m.par.(0) in
        Actor_utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|]
      )
    | Broadcast -> (
        Owl_log.info "%s" ("broadcast @ " ^ !_context.myself_addr);
        Actor_memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
        Actor_utils.send ~bar !_context.master_sock OK [||]
      )
    | Reduce -> (
        Owl_log.info "%s" ("reduce @ " ^ !_context.myself_addr);
        let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
        let y =
          match Actor_memory.find m.par.(1) with
          | hd :: tl -> Some (List.fold_left f hd tl)
          | []       -> None
        in
        Actor_utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|];
      )
    | Fold -> (
        Owl_log.info "%s" ("fold @ " ^ !_context.myself_addr);
        let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
        let y =
          match Actor_memory.find m.par.(1) with
          | hd :: tl -> Some (List.fold_left f hd tl)
          | []       -> None
        in
        Actor_utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|];
      )
    | Pipeline -> (
      Owl_log.info "%s" ("pipelined @ " ^ !_context.myself_addr);
      process_pipeline m.par;
      Actor_utils.send ~bar !_context.master_sock OK [||]
      )
    | Terminate -> (
        Owl_log.info "%s" ("terminate @ " ^ !_context.myself_addr);
        Actor_utils.send ~bar !_context.master_sock OK [||];
        Unix.sleep 1; (* FIXME: sleep ... *)
        failwith ("#" ^ !_context.job_id ^ " terminated")
      )
    | Load -> (
        Owl_log.info "%s" ("load " ^ m.par.(0) ^ " @ " ^ !_context.myself_addr);
        let path = Str.(split (regexp "://")) m.par.(0) in
        let b =
          match (List.nth path 0) with
          | "unix" -> Actor_storage.unix_load (List.nth path 1)
          | _      -> failwith "Load: unknown system!"
        in
        Actor_memory.add m.par.(1) [ b ];
        Actor_utils.send ~bar !_context.master_sock OK [||]
      )
    | Save -> (
        Owl_log.info "%s" ("save " ^ m.par.(0) ^ " @ " ^ !_context.myself_addr);
        let path = Str.(split (regexp "://")) m.par.(0) in
        let c =
          match (List.nth path 0) with
          | "unix" -> Actor_storage.unix_save (List.nth path 1) (Actor_memory.find m.par.(1))
          | _      -> failwith "Save: unknown system!"
        in
        Actor_utils.send ~bar !_context.master_sock OK [|Marshal.to_string c []|]
      )
    | _ -> (
        Owl_log.info "%s" ("Buffering " ^ !_context.myself_addr ^ " <- " ^ i ^ " m.bar : " ^ string_of_int (m.bar));
        Hashtbl.add !_context.msbuf m.bar (i,m)
      )
  done with Failure e -> (
    Owl_log.warn "%s" e;
    Zmq.Socket.(close !_context.master_sock; close !_context.myself_sock);
    Pervasives.exit 0 )


let init m context =
  _context := context;
  !_context.master_addr <- m.par.(0);
  (* connect to job master *)
  let master = Zmq.Socket.create !_context.ztx Zmq.Socket.dealer in
  Zmq.Socket.set_send_high_water_mark master Actor_config.high_warter_mark;
  Zmq.Socket.set_identity master !_context.myself_addr;
  Zmq.Socket.connect master !_context.master_addr;
  Actor_utils.send master OK [|!_context.myself_addr|];
  !_context.master_sock <- master;
  (* enter into worker service loop *)
  service_loop ()
