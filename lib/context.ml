(** [ Context ]
  maintain a context for each applicatoin
*)

type t = {
  manager : string;
  id : string;
}

let _context = ref {
  manager = "";
  id = "";
}

let init id url =
  _context := { manager = url; id = id; };
  let context = ZMQ.Context.create () in
  let requester = ZMQ.Socket.create context ZMQ.Socket.req in
  ZMQ.Socket.connect requester url;
  ZMQ.Socket.send requester id;
  let role = ZMQ.Socket.recv requester in
  match role with
    | "master" -> print_endline "master node"
    | "worker" -> print_endline "worker node"
    | _ -> print_endline "unknown command";
  ZMQ.Socket.close requester;
  ZMQ.Context.terminate context


let map f = None

let reduce f = None

let collect f = None

let execute f = None
