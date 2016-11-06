(** [ Data Parallel ] Map-Reduce module *)

open Types

(* update config information *)
let _ = Logger.update_config Config.level Config.logdir ""

let init jid url =
  let _ztx = ZMQ.Context.create () in
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  let _context = Utils.create_context jid _addr _router _ztx in
  match m.typ with
    | Job_Master -> Mapreserver.init m _context
    | Job_Worker -> Mapreclient.init m _context
    | _ -> Logger.info "%s" "unknown command";
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
