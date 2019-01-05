(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module type Sig = sig

  type socket


  val init : unit -> unit Lwt.t

  val exit : unit -> unit Lwt.t

  val socket : unit -> socket

  val listen : string -> socket

  val connect : socket -> string -> unit Lwt.t

  val send : socket -> string -> unit Lwt.t

  val recv : socket -> string Lwt.t

  val close : socket -> unit Lwt.t

end
