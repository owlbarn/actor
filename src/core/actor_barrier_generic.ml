(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type t = {
  promise  : unit Lwt.t;
  resolver : unit Lwt.u;
  check    : unit -> bool;
  callback : unit -> unit Lwt.t;
  data     : (string, string) Hashtbl.t;
}


let bars : (int, t) Hashtbl.t = Hashtbl.create 128


let make bar_id check callback data =
  let promise, resolver = Lwt.wait () in
  let bar_rec = { promise; resolver; check; callback; data } in
  Hashtbl.add bars bar_id bar_rec


let remove bar_id = Hashtbl.remove bars bar_id


let wait bar_id =
  let bar = Hashtbl.find bars bar_id in
  let%lwt () = bar.promise in
  let%lwt () = bar.callback () in
  Lwt.return ()


let check bar_id =
  let bar = Hashtbl.find bars bar_id in
  bar.check ()


let wakeup bar_id =
  let bar = Hashtbl.find bars bar_id in
  Lwt.wakeup bar.resolver ()


let get bar_id = Hashtbl.find bars bar_id
