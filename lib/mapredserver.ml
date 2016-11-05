
open Types

(* some global varibles *)
let _context = { jid = ""; master = ""; worker = StrMap.empty }

let init m jid _addr _router _ztx =
  _context.jid <- jid;
  _context.master <- _addr;
  (* contact allocated actors to assign jobs *)
  let addrs = Marshal.from_string m.par.(0) 0 in
  List.map (fun x ->
    let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
    ZMQ.Socket.connect req x;
    let app = Filename.basename Sys.argv.(0) in
    let arg = Marshal.to_string Sys.argv [] in
    Utils.send req Job_Create [|_addr; app; arg|]; req
  ) addrs |> List.iter ZMQ.Socket.close;
  (* wait until all the allocated actors register *)
  while (StrMap.cardinal _context.worker) < (List.length addrs) do
    let i, m = Utils.recv _router in
    let s = ZMQ.Socket.create _ztx ZMQ.Socket.dealer in
    ZMQ.Socket.connect s m.par.(0);
    _context.worker <- (StrMap.add m.par.(0) s _context.worker);
  done
