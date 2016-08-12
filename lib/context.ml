(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  mutable jid : string;
  mutable master : string;
  mutable workers : string list;
}

let _context = {
  jid = "";
  master = "";
  workers = [];
}

let my_addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 5000 + 5000))
let _ztx = ZMQ.Context.create ()

let master_fun m =
  print_endline "[master]: init the job";
  _context.master <- my_addr;
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  (* TODO: retrieve actors from manager, then let actors start worker process *)
  for i = 0 to 1 do (* FIXME: only allow two workers *)
    let m = ZMQ.Socket.recv rep in
    _context.workers <- (m :: _context.workers);
    print_endline m;
    ZMQ.Socket.send rep "ok"
  done;
  ZMQ.Socket.close rep

let worker_fun m =
  _context.master <- m.par.(0);
  (* connect to job master *)
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req _context.master;
  ZMQ.Socket.send req my_addr;
  ZMQ.Socket.close req;
  (* TODO: connect to local actor *)
  (* set up job worker *)
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv rep) in
    match m.typ with
    | MapTask -> (
      Utils.logger ("map @ " ^ my_addr);
      ZMQ.Socket.send rep "ok";
      let f : 'a -> 'b = Marshal.from_string m.par.(0) 0 in
      let y = f (Dfs.find m.par.(1)) in
      Dfs.add (m.par.(2)) y
      )
    | CollectTask -> (
      Utils.logger ("collect @ " ^ my_addr);
      let y = Marshal.to_string (Dfs.find m.par.(0)) [] in
      ZMQ.Socket.send rep y
      )
    | BroadcastTask -> (
      Utils.logger ("broadcast @ " ^ my_addr);
      ZMQ.Socket.send rep "ok";
      Dfs.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      )
    | Terminate -> (
      Utils.logger ("terminate @ " ^ my_addr);
      ZMQ.Socket.send rep "ok";
      failwith "terminated"
      )
    | _ -> ()
  done;
  ZMQ.Socket.close rep

let init jid url =
  _context.jid <- jid;
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  ZMQ.Socket.send req (to_msg Job_Reg [|my_addr; jid|]);
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> master_fun m
    | Job_Worker -> worker_fun m
    | _ -> Utils.logger "unknown command";
  ZMQ.Socket.close req;
  ZMQ.Context.terminate _ztx

let map f x =
  Printf.printf "[master]: map -> %i workers\n" (List.length _context.workers);
  let y = Dfs.rand_id () in
  List.iter (fun w ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req w;
    let g = Marshal.to_string f [ Marshal.Closures ] in
    ZMQ.Socket.send req (to_msg MapTask [|g; x; y|]);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req;
    ) _context.workers; y

let collect x =
  Printf.printf "[master]: collect -> %i workers\n" (List.length _context.workers);
  List.map (fun w ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req w;
    ZMQ.Socket.send req (to_msg CollectTask [|x|]);
    let y = ZMQ.Socket.recv req in
    ZMQ.Socket.close req;
    Marshal.from_string y 0
    ) _context.workers

let terminate () =
  Printf.printf "[master]: terminate -> %i workers\n" (List.length _context.workers);
  List.iter (fun w ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req w;
    ZMQ.Socket.send req (to_msg Terminate [||]);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req
    ) _context.workers

let broadcast x =
  Printf.printf "[master]: broadcast -> %i workers\n" (List.length _context.workers);
  let y = Dfs.rand_id () in
  List.iter (fun w ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req w;
    ZMQ.Socket.send req (to_msg BroadcastTask [|Marshal.to_string x []; y|]);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req
    ) _context.workers; y

let get_value x = Dfs.find x
