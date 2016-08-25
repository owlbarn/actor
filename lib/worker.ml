(** [ Actor ]
  connect to Manager, represent a working node/actor.
*)

open Types

let _clock = ref 0
let manager = "tcp://127.0.0.1:5555"
let addr = "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000))
let myid = "actor_" ^ (string_of_int (Random.int 9000 + 1000))
let _ztx = ZMQ.Context.create ()

let register req id u_addr m_addr =
  Utils.logger ("register -> " ^ m_addr);
  ZMQ.Socket.send req (to_msg !_clock User_Reg [|id; u_addr|]);
  ignore (ZMQ.Socket.recv req)

let heartbeat req id u_addr m_addr =
  Utils.logger ("heartbeat -> " ^ m_addr);
  ZMQ.Socket.send req (to_msg !_clock Heartbeat [|id; u_addr|]);
  ignore (ZMQ.Socket.recv req)

let start_app app arg =
  Utils.logger ("starting job " ^ app);
  match Unix.fork () with
  | 0 -> if Unix.fork () = 0 then Unix.execv app arg else exit 0
  | p -> ignore(Unix.wait ())

let deploy_app x = Utils.logger "error, cannot find app!"

let run id u_addr m_addr =
  (* set up connection to manager *)
  let req = ZMQ.Socket.create _ztx ZMQ.Socket.req in
  ZMQ.Socket.connect req m_addr;
  register req myid u_addr m_addr;
  (* set up local service *)
  let rep = ZMQ.Socket.create _ztx ZMQ.Socket.rep in
  ZMQ.Socket.bind rep u_addr;
  while true do
    ZMQ.Socket.set_receive_timeout rep (300 * 1000);
    try let m = of_msg (ZMQ.Socket.recv rep) in
      match m.typ with
      | Job_Create -> (
        let app = m.par.(1) in
        let arg = Marshal.from_string m.par.(2) 0 in
        Utils.logger (app ^ " <- " ^ m.par.(0));
        ZMQ.Socket.send rep (Marshal.to_string OK []);
        match Sys.file_exists app with
        | true ->  start_app app arg
        | false -> deploy_app app
        )
      | _ -> ()
    with
      | Unix.Unix_error (_,_,_) -> heartbeat req id u_addr m_addr
      | ZMQ.ZMQ_exception (_,s) -> Utils.logger ("error, " ^ s)
      | exn -> Utils.logger "unknown error"
  done;
  ZMQ.Socket.close rep;
  ZMQ.Socket.close req;
  ZMQ.Context.terminate _ztx

let () = run myid addr manager
