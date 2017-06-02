(** Some shared helper functions *)

open Actor_types

let recv s =
  let m = ZMQ.Socket.recv_all ~block:true s in
  (List.nth m 0, List.nth m 1 |> of_msg)

let send ?(bar=0) v t s =
  try ZMQ.Socket.send ~block:false v (to_msg bar t s)
  with exn -> let hwm = ZMQ.Socket.get_send_high_water_mark v in
  Actor_logger.error "fail to send bar:%i hwm:%i" bar hwm

let rec _bind_available_addr addr sock ztx =
  addr := "tcp://127.0.0.1:" ^ (string_of_int (Random.int 10000 + 50000));
  try ZMQ.Socket.bind sock !addr
  with exn -> _bind_available_addr addr sock ztx

let bind_available_addr ztx =
  let router : [`Router] ZMQ.Socket.t = ZMQ.Socket.create ztx ZMQ.Socket.router in
  let addr = ref "" in _bind_available_addr addr router ztx;
  ZMQ.Socket.set_receive_high_water_mark router Actor_config.high_warter_mark;
  !addr, router

(* the following 3 functions are for shuffle operations *)

let _group_by_key x =
  let h = Hashtbl.create 1_024 in
  List.iter (fun (k,v) ->
    match Hashtbl.mem h k with
    | true  -> Hashtbl.replace h k ((Hashtbl.find h k) @ [v])
    | false -> Hashtbl.add h k [v]
  ) x;
  Hashtbl.fold (fun k v l -> (k,v) :: l) h []

let group_by_key x = (* FIXME: stack overflow if there too many values for a key *)
  let h, g = Hashtbl.(create 1_024, create 1_024) in
  List.iter (fun (k,v) -> Hashtbl.(add h k v; if not (mem g k) then add g k None)) x;
  Hashtbl.fold (fun k _ l -> (k,Hashtbl.find_all h k) :: l) g []

let flatten_kvg x =
  try List.map (fun (k,l) -> List.map (fun v -> (k,v)) l) x |> List.flatten
  with exn -> print_endline "Error: flatten_kvg"; []

let choose_load x n i = List.filter (fun (k,l) -> (Hashtbl.hash k mod n) = i) x

(* generate a log file name from address *)
let addr_to_log x =
  let path = Str.(split (regexp "://")) x in
  List.nth path 1 |> Str.(global_replace (regexp "[:.]") "_")

let empty_mapre_context () =
  let ztx = ZMQ.Context.create () in
  {
    ztx         = ztx;
    job_id      = "";
    master_addr = "";
    myself_addr = "";
    master_sock = ZMQ.Socket.(create ztx dealer);
    myself_sock = ZMQ.Socket.(create ztx router);
    workers     = StrMap.empty;
    step        = 0;
    msbuf       = Hashtbl.create 256;
  }

let empty_param_context () =
  let ztx = ZMQ.Context.create () in
  {
    ztx         = ztx;
    job_id      = "";
    master_addr = "";
    myself_addr = "";
    master_sock = ZMQ.Socket.(create ztx dealer);
    myself_sock = ZMQ.Socket.(create ztx router);
    workers     = StrMap.empty;
    step        = 0;
    stale       = 1;
    worker_busy = Hashtbl.create 1_000;
    worker_step = Hashtbl.create 1_000;
    step_worker = Hashtbl.create 1_000;
  }

let empty_peer_context () =
  let ztx = ZMQ.Context.create () in
  {
    ztx         = ztx;
    job_id      = "";
    master_addr = "";
    myself_addr = "";
    master_sock = ZMQ.Socket.(create ztx dealer);
    myself_sock = ZMQ.Socket.(create ztx router);
    workers     = StrMap.empty;
    step        = 0;
    block       = false;
    mpbuf       = [];
    spbuf       = Hashtbl.create 32;
  }
