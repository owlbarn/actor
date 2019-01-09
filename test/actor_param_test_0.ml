(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Impl = struct

  type model = (string, int) Hashtbl.t

  type key = string

  type value = int

  let model : model =
    let htbl = Hashtbl.create 128 in
    Hashtbl.add htbl "a" 0;
    Hashtbl.add htbl "b" 0;
    Hashtbl.add htbl "c" 0;
    htbl

  let get keys =
    Array.map (Hashtbl.find model) keys

  let set kv_pairs =
    Array.iter (fun (key, value) ->
      Hashtbl.replace model key value
    ) kv_pairs

  let schd nodes =
    Array.map (fun node ->
      let key = "a" in
      let value = (get [|key|]).(0) in
      let tasks = [|(key, value)|] in
      (node, tasks)
    ) nodes

  let push kv_pairs =
    Unix.sleep (Random.int 3);
    Array.map (fun (key, value) ->
      let new_value = value + Random.int 10 in
      Owl_log.info "%s: %i => %i" key value new_value;
      (key, new_value)
    ) kv_pairs

  let pull updates = updates

end


include Actor_param_types.Make(Impl)

module M = Actor_param.Make (Actor_net_zmq) (Actor_sys_unix) (Impl)


let main args =
  Owl_log.(set_level DEBUG);
  Random.self_init ();

  (* define server uuid and address *)
  let server_uuid = "server" in
  let server_addr = "tcp://127.0.0.1:5555" in

  (* define my own uuid and address *)
  let my_uuid = args.(1) in
  let my_addr =
    if my_uuid = server_uuid then
      server_addr
    else
      let port = string_of_int (6000 + Random.int 1000) in
      "tcp://127.0.0.1:" ^ port
  in

  (* define the participants *)
  let book = Actor_book.make () in
  Actor_book.add book "w0" "" false (-1);
  Actor_book.add book "w1" "" false (-1);
  if my_uuid <> server_uuid then
    Actor_book.set_addr book my_uuid my_addr;

  (* define parameter server context *)
  let context = {
    my_uuid;
    my_addr;
    server_uuid;
    server_addr;
    book;
  }
  in

  (* start the event loop *)
  Lwt_main.run (M.init context)


let _ =
  main Sys.argv
