(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module B =
  Actor_barrier_generic.Make(struct
    type param = (string, string) Hashtbl.t
  end)


let check bar = B.(Hashtbl.length bar.param = 0)


let make iter_idx client callback =
  let bar_id = iter_idx in
  let htbl = Actor_param_utils.arr_to_htbl client in
  B.make bar_id callback htbl


let wait iter_idx = B.wait iter_idx


let sync bar_id node_id =
  if B.mem bar_id then (
    let bar = B.get bar_id in
    Hashtbl.remove bar.param node_id;

    if check bar then
      B.wakeup bar_id
  )
