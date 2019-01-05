(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type socket = [`Dealer] Zmq_lwt.Socket.t


let context =
  ref (Zmq.Context.create ())


let init () =
  context := Zmq.Context.create ();
  Lwt.return ()


let exit () =
  Zmq.Context.terminate !context;
  Lwt.return ()


let socket () =
  let sock = Zmq.Socket.create !context Zmq.Socket.dealer in
  Zmq_lwt.Socket.of_socket sock


let listen addr =
  let sock = Zmq.Socket.create !context Zmq.Socket.dealer in
  Zmq.Socket.bind sock addr;
  Zmq_lwt.Socket.of_socket sock


let connect sock addr =
  let sock = Zmq_lwt.Socket.to_socket sock in
  Zmq.Socket.connect sock addr;
  Lwt.return ()


let send sock msg =
  Zmq_lwt.Socket.send sock msg


let recv sock =
  Zmq_lwt.Socket.recv sock


let close sock = Zmq_lwt.Socket.close sock
