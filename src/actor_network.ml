(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module type Sig = sig

  module Context : sig
    type t
  end

  module Socket : sig
    type t
  end

end
