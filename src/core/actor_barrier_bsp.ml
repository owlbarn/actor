(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

open Actor_book


let pass book =
  let fastest = ref min_int in
  let synced = ref true in

  Hashtbl.iter (fun _ node ->
    if node.step > !fastest then
      fastest := node.step;
    if !fastest > min_int && !fastest != node.step then
      synced := false
  ) book;

  if !synced then
    Actor_param_utils.htbl_to_arr book
    |> Array.map fst
  else
    [| |]


let sync book uuid =
  let step = Actor_book.get_step book uuid in
  Actor_book.set_step book uuid (step + 1)
