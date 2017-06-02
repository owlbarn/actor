(** [ Peer-to-Peer Parallel ] Client module *)

open Actor_types

(* the global context: master, worker, etc. *)
let _context = ref (Actor_utils.empty_peer_context ())

(* default schedule function *)
let _default_schedule = fun _ -> [ ]
let _schedule = ref (Marshal.to_string _default_schedule [ Marshal.Closures ])

(* default push function *)
let _default_push = fun _ _ -> []
let _push = ref (Marshal.to_string _default_push [ Marshal.Closures ])

(* default stopping function *)
let _default_stop = fun _ -> false
let _stop = ref (Marshal.to_string _default_stop [ Marshal.Closures ])

let _get k =
  let k = Marshal.to_string k [] in
  let s = [|k; !_context.master_addr|] in
  Actor_utils.send !_context.master_sock P2P_Get s;
  let _, m = Actor_utils.recv !_context.myself_sock in
  let k, v, t = Marshal.from_string m.par.(0) 0 in
  v, t

let _set k v =
  let s = Marshal.to_string (k, v, -1) [] in
  Actor_utils.send !_context.master_sock P2P_Set [|s|]

let _push_model params =
  let s = Marshal.to_string params [] in
  Actor_utils.send !_context.master_sock P2P_Push [|s|]

let _pull_model params =
  List.map (fun k -> let v, _ = _get k in (k,v)) params

let _pull_model_batch params =
  let s = Marshal.to_string params [] in
  Actor_utils.send !_context.master_sock P2P_Pull [|s|];
  let _, m = Actor_utils.recv !_context.myself_sock in
  let kvs = Marshal.from_string m.par.(0) 0 in
  kvs

let _barrier () =
  Actor_utils.send !_context.master_sock P2P_Bar [||];
  let _, m = Actor_utils.recv !_context.myself_sock in
  !_context.step <- m.bar

let service_loop () =
  Actor_logger.debug "p2p_client @ %s" !_context.master_addr;
  (* unmarshal the schedule and push function *)
  let schedule : 'a p2p_schedule_typ = Marshal.from_string !_schedule 0 in
  let push : ('a, 'b) p2p_push_typ = Marshal.from_string !_push 0 in
  let stop : p2p_stop_typ = Marshal.from_string !_stop 0 in
  (* loop to process messages *)
  try while not (stop _context) do
    schedule _context
    |> _pull_model
    |> push _context
    |> _push_model
    |> _barrier
  done with Failure e -> (
    Actor_logger.warn "%s" e;
    ZMQ.Socket.close !_context.myself_sock;
    Pervasives.exit 0 )

let init m context =
  _context := context;
  (* re-initialise since it is a new process *)
  !_context.ztx <- ZMQ.Context.create ();
  !_context.master_addr <- context.myself_addr;
  let _addr, _router = Actor_utils.bind_available_addr !_context.ztx in
  !_context.myself_addr <- _addr;
  !_context.myself_sock <- _router;
  (* set up local p2p server <-> client *)
  let sock = ZMQ.Socket.create !_context.ztx ZMQ.Socket.dealer in
  ZMQ.Socket.set_send_high_water_mark sock Actor_config.high_warter_mark;
  ZMQ.Socket.set_identity sock !_context.myself_addr;
  ZMQ.Socket.connect sock !_context.master_addr;
  !_context.master_sock <- sock;
  Actor_utils.send !_context.master_sock P2P_Connect [|!_context.myself_addr|];
  (* enter into client service loop *)
  service_loop ()
