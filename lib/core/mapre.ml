(** [ Data Parallel ] Map-Reduce module *)

open Actor_types

let init jid url =
  let _ztx = ZMQ.Context.create () in
  let _addr, _router = Actor_utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Actor_utils.send req Job_Reg [|_addr; jid|];
  (* create and initialise part of the context *)
  let _context = Actor_utils.empty_mapre_context () in
  _context.job_id <- jid;
  _context.myself_addr <- _addr;
  _context.myself_sock <- _router;
  _context.ztx <- _ztx;
  (* depends on the role, start server or client *)
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
  | Job_Master -> Mapreserver.init m _context
  | Job_Worker -> Mapreclient.init m _context
  | _ -> Actor_logger.info "%s" "unknown command";
  ZMQ.Socket.close req

(* interface to mapreserver functions *)

let map = Mapreserver.map

let flatmap = Mapreserver.flatmap

let reduce = Mapreserver.reduce

let reduce_by_key = Mapreserver.reduce_by_key

let fold = Mapreserver.fold

let filter = Mapreserver.filter

let flatten = Mapreserver.flatten

let shuffle = Mapreserver.shuffle

let union = Mapreserver.union

let join = Mapreserver.join

let broadcast = Mapreserver.broadcast

let get_value = Mapreserver.get_value

let count = Mapreserver.count

let collect = Mapreserver.collect

let terminate = Mapreserver.terminate

let apply = Mapreserver.apply

let load = Mapreserver.load

let save = Mapreserver.save
