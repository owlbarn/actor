(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module type Sig = sig

  type socket


  val init : unit -> unit

  val exit : unit -> unit

  val socket : unit -> socket

  val listen : string -> socket

  val connect : socket -> string -> unit

  val send : socket -> string -> unit

  val recv : socket -> string

  val close : socket -> unit

  val timeout : socket -> int -> unit

end
