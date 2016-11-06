
open Types

(* some global varibles *)
let _context = { jid = ""; master = ""; worker = StrMap.empty }
let _msgbuf = Hashtbl.create 1024
let _master : [`Dealer] ZMQ.Socket.t array ref = ref [||]
let _ztx = ref [||]

(* update config information *)
let _ = Logger.update_config Config.level Config.logdir ""

let barrier _router bar = Barrier.bsp bar _router _context.worker _msgbuf

let shuffle _addr bar x z =
  List.mapi (fun i k ->
    let v = Utils.choose_load x (List.length z) i in
    let s = if StrMap.mem k _context.worker then
      StrMap.find k _context.worker
    else (
      let s = ZMQ.Socket.(create !_ztx.(0) dealer) in
      let _ = ZMQ.Socket.(set_identity s _addr; connect s k) in
      let _ = _context.worker <- StrMap.add k s _context.worker in
      let _ = ZMQ.Socket.set_send_high_water_mark s Config.high_warter_mark in
      s ) in
    Utils.send ~bar s OK [|Marshal.to_string v []|]
  ) z

let process_pipeline _addr _router s =
  Array.iter (fun s ->
    let m = of_msg s in
    match m.typ with
    | MapTask -> (
      Logger.info "%s" ("map @ " ^ _addr);
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      List.map f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FilterTask -> (
      Logger.info "%s" ("filter @ " ^ _addr);
      let f : 'a -> bool = Marshal.from_string m.par.(0) 0 in
      List.filter f (Memory.find m.par.(1)) |> Memory.add m.par.(2)
      )
    | FlattenTask -> (
      Logger.info "%s" ("flatten @ " ^ _addr);
      List.flatten (Memory.find m.par.(0)) |> Memory.add m.par.(1)
      )
    | UnionTask -> (
      Logger.info "%s" ("union @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Memory.add m.par.(2)
      )
    | ReduceByKeyTask -> (
      Logger.info "%s" ("reduce_by_key @ " ^ _addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      Memory.find m.par.(1) |> Utils.group_by_key |> List.map (fun (k,l) ->
        match l with
        | hd :: tl -> (k, List.fold_left f hd tl)
        | [] -> failwith "error in reduce"
      ) |> Memory.add m.par.(2)
      )
    | JoinTask -> (
      Logger.info "%s" ("join @ " ^ _addr);
      (Memory.find m.par.(0)) @ (Memory.find m.par.(1))
      |> Utils.group_by_key |> Memory.add m.par.(2)
      )
    | ShuffleTask -> (
      Logger.info "%s" ("shuffle @ " ^ _addr);
      let x = Memory.find m.par.(0) |> Utils.group_by_key in
      let z = Marshal.from_string m.par.(2) 0 in
      let bar = Marshal.from_string m.par.(3) 0 in
      let _ = shuffle _addr bar x z in
      barrier _router bar
      |> List.map (fun m -> Marshal.from_string m.par.(0) 0 |> Utils.flatten_kvg)
      |> List.flatten |> Memory.add m.par.(1);
      )
    | _ -> Logger.info "%s" "unknown task types"
  ) s

let service_loop _addr _router =
  Logger.debug "mapre worker @ %s" _addr;
  (* set up local loop of a job worker *)
  try while true do
    let i, m = Utils.recv _router in
    let bar = m.bar in
    match m.typ with
    | Count -> (
      Logger.info "%s" ("count @ " ^ _addr);
      let y = List.length (Memory.find m.par.(0)) in
      Utils.send ~bar !_master.(0) OK [|Marshal.to_string y []|]
      )
    | Collect -> (
      Logger.info "%s" ("collect @ " ^ _addr);
      let y = Memory.find m.par.(0) in
      Utils.send ~bar !_master.(0) OK [|Marshal.to_string y []|]
      )
    | Broadcast -> (
      Logger.info "%s" ("broadcast @ " ^ _addr);
      Memory.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      Utils.send ~bar !_master.(0) OK [||]
      )
    | Reduce -> (
      Logger.info "%s" ("reduce @ " ^ _addr);
      let f : 'a -> 'a -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar !_master.(0) OK [|Marshal.to_string y []|];
      )
    | Fold -> (
      Logger.info "%s" ("fold @ " ^ _addr);
      let f : 'a -> 'b -> 'a = Marshal.from_string m.par.(0) 0 in
      let y = match Memory.find m.par.(1) with
      | hd :: tl -> Some (List.fold_left f hd tl) | [] -> None
      in Utils.send ~bar !_master.(0) OK [|Marshal.to_string y []|];
      )
    | Pipeline -> (
      Logger.info "%s" ("pipelined @ " ^ _addr);
      process_pipeline _addr _router m.par;
      Utils.send ~bar !_master.(0) OK [||]
      )
    | Terminate -> (
      Logger.info "%s" ("terminate @ " ^ _addr);
      Utils.send ~bar !_master.(0) OK [||];
      Unix.sleep 1; (* FIXME: sleep ... *)
      failwith ("#" ^ _context.jid ^ " terminated")
      )
    | Load -> (
      Logger.info "%s" ("load " ^ m.par.(0) ^ " @ " ^ _addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let b = match (List.nth path 0) with
      | "unix"  -> Storage.unix_load (List.nth path 1)
      | _ -> Logger.info "%s" ("Error: unknown system!"); "" in
      Memory.add m.par.(1) [ b ];
      Utils.send ~bar !_master.(0) OK [||]
      )
    | Save -> (
      Logger.info "%s" ("save " ^ m.par.(0) ^ " @ " ^ _addr);
      let path = Str.(split (regexp "://")) m.par.(0) in
      let c = match (List.nth path 0) with
      | "unix"  -> Storage.unix_save (List.nth path 1) (Memory.find m.par.(1))
      | _ -> Logger.info "%s" ("Error: unknown system!"); 0 in
      Utils.send ~bar !_master.(0) OK [|Marshal.to_string c []|]
      )
    | _ -> (
      Logger.debug "%s" ("Buffering " ^ _addr ^ " <- " ^ i ^ " m.bar : " ^ string_of_int (m.bar));
      Hashtbl.add _msgbuf m.bar (i,m)
      )
  done with Failure e -> (
    Logger.warn "%s" e;
    ZMQ.Socket.(close !_master.(0); close _router);
    Pervasives.exit 0 )

let init m jid _addr _router ztx =
  _context.jid <- jid;
  _context.master <- m.par.(0);
  (* connect to job master *)
  let master = ZMQ.Socket.create ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark master Config.high_warter_mark;
  ZMQ.Socket.set_identity master _addr;
  ZMQ.Socket.connect master m.par.(0);
  Utils.send master OK [|_addr|];
  _master := [|master|];
  _ztx := [|ztx|];
  (* enter into worker service loop *)
  service_loop _addr _router
