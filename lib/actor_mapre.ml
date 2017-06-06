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
  let _ = match m.typ with
    | Job_Master -> Actor_mapreserver.init m _context
    | Job_Worker -> Actor_mapreclient.init m _context
    | _ -> Actor_logger.info "%s" "unknown command"
  in
  ZMQ.Socket.close req

(* interface to mapreserver functions *)

let map = Actor_mapreserver.map

let map_partition = Actor_mapreserver.map_partition

let flatmap = Actor_mapreserver.flatmap

let reduce = Actor_mapreserver.reduce

let reduce_by_key = Actor_mapreserver.reduce_by_key

let fold = Actor_mapreserver.fold

let filter = Actor_mapreserver.filter

let flatten = Actor_mapreserver.flatten

let shuffle = Actor_mapreserver.shuffle

let union = Actor_mapreserver.union

let join = Actor_mapreserver.join

let broadcast = Actor_mapreserver.broadcast

let get_value = Actor_mapreserver.get_value

let count = Actor_mapreserver.count

let collect = Actor_mapreserver.collect

let terminate = Actor_mapreserver.terminate

let apply = Actor_mapreserver.apply

let load = Actor_mapreserver.load

let save = Actor_mapreserver.save

(* experimental functions  *)

let workers = Actor_mapreserver.workers

let myself () =
  match Actor_mapreserver.(!_context.job_id) = "" with
  | true  -> Actor_mapreclient.(!_context.myself_addr)
  | false -> Actor_mapreserver.(!_context.myself_addr)
