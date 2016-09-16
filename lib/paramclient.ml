(** [ Parameter ]
  provides an interface to global varialbe like KV store
*)

open Types

let _ztx = ZMQ.Context.create ()
let _ps : [`Dealer] ZMQ.Socket.t = ZMQ.Socket.(create _ztx dealer)
let _ = ZMQ.Socket.connect _ps Config.ps_addr

let get k t =
  Logger.info "%s" ("GET @ ???");
  let k' = Marshal.to_string k [] in
  Utils.send ~bar:t _ps PS_Set [|k'|]

let set k v t =
  Logger.info "%s" ("SET @ ???");
  let k' = Marshal.to_string k [] in
  let v' = Marshal.to_string v [] in
  Utils.send ~bar:t _ps PS_Set [|k'; v'|]
