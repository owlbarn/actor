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

let master_fun m ctx =
  print_endline "[master] init the job";
  _context.master <- my_addr;
  let rep = ZMQ.Socket.create ctx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep my_addr;
  for i = 0 to 2 do (* FIXME: only allow three workers *)
    let m = ZMQ.Socket.recv rep in
    print_endline m;
    ZMQ.Socket.send rep "ok"
  done;
  ZMQ.Socket.close rep

let worker_fun m ctx =
  _context.master <- m.str;
  let req = ZMQ.Socket.create ctx ZMQ.Socket.req in
  ZMQ.Socket.connect req _context.master;
  ZMQ.Socket.send req my_addr;
  ignore (ZMQ.Socket.recv req);
  while true do
    print_endline "worker ...";
    Unix.sleep 5
  done

let init jid url =
  _context.jid <- jid;
  let ctx = ZMQ.Context.create () in
  let req = ZMQ.Socket.create ctx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  ZMQ.Socket.send req (to_msg Job_Reg my_addr jid);
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> master_fun m ctx
    | Job_Worker -> worker_fun m ctx
    | _ -> print_endline "unknown command";
  ZMQ.Socket.close req;
  ZMQ.Context.terminate ctx

let map f = None

let reduce f = None

let collect f = None

let execute f = None
