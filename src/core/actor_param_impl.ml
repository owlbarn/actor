(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


module type Sig = sig

  type model

  type key

  type value

  val get : model -> key -> value

  val set : model -> key -> value -> unit

  val schedule : string array -> (string * key) array

  val push : key -> value

end
