(** [ Context ]
  maintain a context for each applicatoin
*)

open Types

type t = {
  manager : string;
  jid : string;
}

let _context = ref {
  manager = "tcp://*:6666";
  jid = "";
}

let master_fun () =
  print_endline "[master] init the job"

let worker_fun () =
  while true do
    print_endline "worker ...";
    Unix.sleep 5
  done

let init uid jid url =
  _context := { manager = url; jid = jid; };
  let context = ZMQ.Context.create () in
  let requester = ZMQ.Socket.create context ZMQ.Socket.req in
  ZMQ.Socket.connect requester url;
  ZMQ.Socket.send requester (to_msg Job_Reg uid jid);
  let role = ZMQ.Socket.recv requester in
  match role with
    | "master" -> master_fun ()
    | "worker" -> worker_fun ()
    | _ -> print_endline "unknown command";
  ZMQ.Socket.close requester;
  ZMQ.Context.terminate context

let map f = None

let reduce f = None

let collect f = None

let execute f = None
