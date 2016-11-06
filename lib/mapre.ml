
open Types

let _ztx = ZMQ.Context.create ()

let init jid url =
  let _addr, _router = Utils.bind_available_addr _ztx in
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req url;
  Utils.send req Job_Reg [|_addr; jid|];
  let m = of_msg (ZMQ.Socket.recv req) in
  match m.typ with
    | Job_Master -> Mapredserver.init m jid _addr _router _ztx
    | Job_Worker -> () (*Mapredclient.init m jid _addr _router _ztx*)
    | _ -> Logger.info "%s" "unknown command";
  ZMQ.Socket.close req
