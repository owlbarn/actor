(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


let check bar =
  Actor_barrier_generic.(Hashtbl.length bar.data = 0)


let wait iter_idx client callback =
  let bar_id = iter_idx in
  let htbl = Actor_param_utils.arr_to_htbl client in
  Actor_barrier_generic.make bar_id callback htbl;
  Actor_barrier_generic.wait bar_id


let sync bar_id node_id =
  let bar = Actor_barrier_generic.get bar_id in
  Hashtbl.remove bar.data node_id;
  if check bar then (
    Actor_barrier_generic.wakeup bar_id
  )
