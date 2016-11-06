
open Types

let _ztx = ZMQ.Context.create ()

let init jid url =
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> Mapreserver.init m jid _addr _router _ztx
    | Job_Worker -> Mapreclient.init m jid _addr _router _ztx
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
