(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  mutable jid : string;
  mutable master : string;
  mutable workers : [ `Req ] ZMQ.Socket.t list;
}

let _context = { jid = ""; master = ""; workers = []; }
let my_addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 5000 + 5000))
let _ztx = ZMQ.Context.create ()

let master_fun m =
  Utils.logger "init the job";
  _context.master <- my_addr;
  (* contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.iter (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    ZMQ.Socket.send req (to_msg Job_Create [|my_addr; app; arg|]);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req
  ) addrs;
  (* wait until all the allocated actors register *)
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  while (List.length _context.workers) < (List.length addrs) do
    let m = ZMQ.Socket.recv rep in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect s m;
    _context.workers <- (s :: _context.workers);
    ZMQ.Socket.send rep "";
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
  try while true do
    let m = of_msg (ZMQ.Socket.recv rep) in
    match m.typ with
    | MapTask -> (
      Utils.logger ("map @ " ^ my_addr);
      ZMQ.Socket.send rep "";
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
      ZMQ.Socket.send rep "";
      Dfs.add m.par.(1) (Marshal.from_string m.par.(0) 0);
      )
    | Terminate -> (
      Utils.logger ("terminate @ " ^ my_addr);
      ZMQ.Socket.send rep ""; Unix.sleep 1; (* FIXME: sleep ... *)
      failwith "terminated"
      )
    | _ -> ()
  done with exn -> (
    Utils.logger "task finished.";
    ZMQ.Socket.close rep;
    Pervasives.exit 0
  )

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
  ZMQ.Socket.close req

let map f x =
  Utils.logger ("map -> " ^ string_of_int (List.length _context.workers) ^ " workers\n");
  let y = Dfs.rand_id () in
  List.iter (fun req ->
    let g = Marshal.to_string f [ Marshal.Closures ] in
    ZMQ.Socket.send req (to_msg MapTask [|g; x; y|]);
    ignore (ZMQ.Socket.recv req)
    ) _context.workers; y

let collect x =
  Utils.logger ("collect -> " ^ string_of_int (List.length _context.workers) ^ " workers\n");
  List.map (fun req ->
    ZMQ.Socket.send req (to_msg CollectTask [|x|]);
    let y = ZMQ.Socket.recv req in
    Marshal.from_string y 0
    ) _context.workers

let terminate () =
  Utils.logger ("terminate -> " ^ string_of_int (List.length _context.workers) ^ " workers\n");
  List.iter (fun req ->
    ZMQ.Socket.send req (to_msg Terminate [||]);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req
    ) _context.workers

let broadcast x =
  Utils.logger ("broadcast -> " ^ string_of_int (List.length _context.workers) ^ " workers\n");
  let y = Dfs.rand_id () in
  List.iter (fun req ->
    ZMQ.Socket.send req (to_msg BroadcastTask [|Marshal.to_string x []; y|]);
    ignore (ZMQ.Socket.recv req)
    ) _context.workers; y

let get_value x = Dfs.find x
