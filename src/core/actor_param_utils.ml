(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


let is_ready book =
  let ready = ref true in
  Hashtbl.iter (fun _ n ->
    Owl_log.error "%s %s" Actor_book.(n.uuid) Actor_book.(n.addr);
    if Actor_book.(String.length n.addr = 0) then
      ready := false
  ) book;
  !ready


let arr_to_htbl arr =
  let len = Array.length arr in
  let htbl = Hashtbl.create len in
  Array.iter (fun k ->
    Hashtbl.add htbl k k
  ) arr;
  htbl


let htbl_to_arr htbl =
  let stack = Owl_utils_stack.make () in
  Hashtbl.iter (fun k v ->
    Owl_utils_stack.push stack (k, v)
  ) htbl;
  Owl_utils_stack.to_array stack
