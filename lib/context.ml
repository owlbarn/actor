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
  print_endline "[master] init the job";
  _context.master <- my_addr;
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  for i = 0 to 1 do (* FIXME: only allow two workers *)
    let m = ZMQ.Socket.recv rep in
    _context.workers <- (m :: _context.workers);
    print_endline m;
    ZMQ.Socket.send rep "ok"
  done;
  ZMQ.Socket.close rep

let worker_fun m =
  _context.master <- m.str;
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req _context.master;
  ZMQ.Socket.send req my_addr;
  ignore (ZMQ.Socket.recv req);
  (* set up worker service *)
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  while true do
    let m = of_msg (ZMQ.Socket.recv rep) in
    match m.typ with
    | Task -> (
      ZMQ.Socket.send rep "ok";
      print_endline ("map @ " ^ my_addr);
      let f : float array -> float array = Marshal.from_string m.str 0 in
      let data = Array.init 5 (fun x -> Random.float 10.) in (* FIXME: test purpose *)
      f data; ()
      )
    | Terminate -> ()
    | _ -> ()
  done;
  ZMQ.Socket.close rep

let init jid url =
  _context.jid <- jid;
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  ZMQ.Socket.send req (to_msg Job_Reg my_addr jid);
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> master_fun m
    | Job_Worker -> worker_fun m
    | _ -> print_endline "unknown command";
  ZMQ.Socket.close req;
  ZMQ.Context.terminate _ztx

let map f x =
  List.iter (fun w ->
    print_endline ("map -> " ^ w);
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req w;
    let s = Marshal.to_string f [ Marshal.Closures ] in
    ZMQ.Socket.send req (to_msg Task x s);
    ignore (ZMQ.Socket.recv req);
    ZMQ.Socket.close req;
    ) _context.workers

let reduce f x = None

let collect f x = None

let execute f x = None
