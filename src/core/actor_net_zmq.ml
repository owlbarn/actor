(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type socket = [`Dealer] Zmq.Socket.t


let context =
  ref (Zmq.Context.create ())


let init () =
  context := Zmq.Context.create ()


let exit () =
  Zmq.Context.terminate !context


let socket () =
  Zmq.Socket.create !context Zmq.Socket.dealer


let listen addr =
  let sock = Zmq.Socket.create !context Zmq.Socket.dealer in
  Zmq.Socket.bind sock addr;
  sock


let connect sock addr =
  Zmq.Socket.connect sock addr


let send sock msg =
  Zmq.Socket.send sock msg


let recv sock =
  Zmq.Socket.recv sock


let close sock = Zmq.Socket.close sock


let timeout sock threshold =
  Zmq.Socket.set_receive_timeout sock threshold
