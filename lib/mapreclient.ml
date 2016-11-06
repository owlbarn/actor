(** [ Data Parallel ] Client module *)

open Types

(* some global varibles *)
let _context = ref (Utils.empty_context ())
let _msgbuf = Hashtbl.create 1024

let barrier bar = Barrier.bsp bar !_context.myself_sock !_context.workers _msgbuf

let shuffle bar x z =
  List.mapi (fun i k ->
    let v = Utils.choose_load x (List.length z) i in
    let s = if StrMap.mem k !_context.workers then
      StrMap.find k !_context.workers
    else (
      let s = ZMQ.Socket.(create !_context.ztx dealer) in
      let _ = ZMQ.Socket.(set_identity s !_context.myself_addr; connect s k) in
      let _ = !_context.workers <- StrMap.add k s !_context.workers in
      let _ = ZMQ.Socket.set_send_high_water_mark s Config.high_warter_mark in
      s ) in
    Utils.send ~bar s OK [|Marshal.to_string v []|]
  ) z

let process_pipeline s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
      Logger.info "%s" ("map @ " ^ !_context.myself_addr);
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      List.map f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FilterTask -> (
      Logger.info "%s" ("filter @ " ^ !_context.myself_addr);
      let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
      List.filter f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FlattenTask -> (
      Logger.info "%s" ("flatten @ " ^ !_context.myself_addr);
      List.flatten (Memory.find m.par.(0)) |> Memory.add m.par.(1)
      )
    | UnionTask -> (
      Logger.info "%s" ("union @ " ^ !_context.myself_addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Memory.add m.par.(2)
      )
    | ReduceByKeyTask -> (
      Logger.info "%s" ("reduce_by_key @ " ^ !_context.myself_addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      Memory.find m.par.(1) |> Utils.group_by_key |> List.map (fun (k,l) ->
        match l with
        | hd :: tl -> (k, List.fold_left f hd tl)
        | [] -> failwith "error in reduce"
      ) |> Memory.add m.par.(2)
      )
    | JoinTask -> (
      Logger.info "%s" ("join @ " ^ !_context.myself_addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Utils.group_by_key |> Memory.add m.par.(2)
      )
    | ShuffleTask -> (
      Logger.info "%s" ("shuffle @ " ^ !_context.myself_addr);
      let x = Memory.find m.par.(0) |> Utils.group_by_key in
      let z = Marshal.from_string m.par.(2) 0 in
      let bar = Marshal.from_string m.par.(3) 0 in
      let _ = shuffle bar x z in
      barrier bar
      |> List.map (fun m -> Marshal.from_string m.par.(0) 0 |> Utils.flatten_kvg)
      |> List.flatten |> Memory.add m.par.(1);
      )
    | _ -> Logger.info "%s" "unknown task types"
  ) s

let service_loop () =
  Logger.debug "mapre worker @ %s" !_context.myself_addr;
  (* set up local loop of a job worker *)
  try while true do
    let i, m = Utils.recv !_context.myself_sock in
    let bar = m.bar in
    match m.typ with
    | Count -> (
      Logger.info "%s" ("count @ " ^ !_context.myself_addr);
      let y = List.length (Memory.find m.par.(0)) in
      Utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|]
      )
    | Collect -> (
      Logger.info "%s" ("collect @ " ^ !_context.myself_addr);
      let y = Memory.find m.par.(0) in
      Utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|]
      )
    | Broadcast -> (
      Logger.info "%s" ("broadcast @ " ^ !_context.myself_addr);
      Memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      Utils.send ~bar !_context.master_sock OK [||]
      )
    | Reduce -> (
      Logger.info "%s" ("reduce @ " ^ !_context.myself_addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|];
      )
    | Fold -> (
      Logger.info "%s" ("fold @ " ^ !_context.myself_addr);
      let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar !_context.master_sock OK [|Marshal.to_string y []|];
      )
    | Pipeline -> (
      Logger.info "%s" ("pipelined @ " ^ !_context.myself_addr);
      process_pipeline m.par;
      Utils.send ~bar !_context.master_sock OK [||]
      )
    | Terminate -> (
      Logger.info "%s" ("terminate @ " ^ !_context.myself_addr);
      Utils.send ~bar !_context.master_sock OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ !_context.job_id ^ " terminated")
      )
    | Load -> (
      Logger.info "%s" ("load " ^ m.par.(0) ^ " @ " ^ !_context.myself_addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let b = match (List.nth path 0) with
      | "unix"  -> Storage.unix_load (List.nth path 1)
      | _ -> Logger.info "%s" ("Error: unknown system!"); "" in
      Memory.add m.par.(1) [ b ];
      Utils.send ~bar !_context.master_sock OK [||]
      )
    | Save -> (
      Logger.info "%s" ("save " ^ m.par.(0) ^ " @ " ^ !_context.myself_addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let c = match (List.nth path 0) with
      | "unix"  -> Storage.unix_save (List.nth path 1) (Memory.find m.par.(1))
      | _ -> Logger.info "%s" ("Error: unknown system!"); 0 in
      Utils.send ~bar !_context.master_sock OK [|Marshal.to_string c []|]
      )
    | _ -> (
      Logger.debug "%s" ("Buffering " ^ !_context.myself_addr ^ " <- " ^ i ^ " m.bar : " ^ string_of_int (m.bar));
      Hashtbl.add _msgbuf m.bar (i,m)
      )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.(close !_context.master_sock; close !_context.myself_sock);
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
