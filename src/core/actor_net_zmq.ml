(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type socket = [`Dealer] Zmq_lwt.Socket.t


let context = ref (Zmq.Context.create ())


let conn_pool : (string, socket) Hashtbl.t = Hashtbl.create 128


let init () =
  Random.self_init ();
  Lwt.return ()


let exit () =
  Hashtbl.iter (fun _ v ->
    Lwt.async (fun () -> Zmq_lwt.Socket.close v)
  ) conn_pool;
  Zmq.Context.terminate !context;
  Lwt.return ()


let socket () =
  let sock = Zmq.Socket.create !context Zmq.Socket.dealer in
  Zmq_lwt.Socket.of_socket sock


let bind sock addr =
  let sock = Zmq_lwt.Socket.to_socket sock in
  Zmq.Socket.bind sock addr;
  Lwt.return ()


let listen addr callback =
  let raw_sock = Zmq.Socket.create !context Zmq.Socket.router in
  Zmq.Socket.bind raw_sock addr;
  let lwt_sock = Zmq_lwt.Socket.of_socket raw_sock in

  let rec loop () =
    let%lwt parts = Zmq_lwt.Socket.recv_all lwt_sock in
    let data = List.nth parts 1 in
    let%lwt () = callback data in
    loop ()
  in

  let%lwt () = loop () in
  Zmq.Socket.close raw_sock;
  Lwt.return ()


let send addr data =
  let sock =
    if Hashtbl.mem conn_pool addr then (
      Hashtbl.find conn_pool addr
    )
    else (
      let raw_sock = Zmq.Socket.create !context Zmq.Socket.dealer in
      Zmq.Socket.connect raw_sock addr;
      let lwt_sock = Zmq_lwt.Socket.of_socket raw_sock in
      Hashtbl.add conn_pool addr lwt_sock;
      lwt_sock
    )
  in
  Zmq_lwt.Socket.send sock data


let recv sock = Zmq_lwt.Socket.recv sock


let close sock = Zmq_lwt.Socket.close sock
