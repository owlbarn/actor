(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module type Sig = sig

  val exec : string -> string array -> unit

  val file_exists : string -> bool Lwt.t

  val sleep : float -> unit Lwt.t

end
