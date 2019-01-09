(*
 * Light Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type t = (string, node) Hashtbl.t

and node = {
  mutable uuid : string;
  mutable addr : string;
  mutable busy : bool;
  mutable step : int;
}


let make () : t = Hashtbl.create 128


let make_node uuid addr busy step = {
  uuid;
  addr;
  busy;
  step;
}


let add book uuid addr busy step =
  if not (Hashtbl.mem book uuid) then (
    let n = make_node uuid addr busy step in
    Hashtbl.add book uuid n
  )


let remove book uuid =
  if Hashtbl.mem book uuid then
    Hashtbl.remove book uuid


let get_addr book uuid =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.addr
  else
    failwith ("Actor_book.get_addr " ^ uuid)


let set_addr book uuid addr =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.addr <- addr
  else
    failwith ("Actor_book.set_addr " ^ uuid)



let get_busy book uuid =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.busy
  else
    failwith ("Actor_book.get_busy " ^ uuid)


let set_busy book uuid busy =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.busy <- busy
  else
    failwith ("Actor_book.set_busy " ^ uuid)


let get_step book uuid =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.step
  else
    failwith ("Actor_book.get_step " ^ uuid)


let set_step book uuid step =
  if Hashtbl.mem book uuid then
    let n = Hashtbl.find book uuid in
    n.step <- step
  else
    failwith ("Actor_book.set_step " ^ uuid)
