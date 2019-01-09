(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module type Sig = sig

  type socket


  val init : unit -> unit Lwt.t

  val exit : unit -> unit Lwt.t

  val listen : string -> (string -> unit Lwt.t) -> unit Lwt.t

  val send : string -> string -> unit Lwt.t

  val recv : socket -> string Lwt.t

  val close : socket -> unit Lwt.t

end
