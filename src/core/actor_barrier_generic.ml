(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

module Make (A : sig type param end) = struct

  type t = {
    promise  : unit Lwt.t;
    resolver : unit Lwt.u;
    callback : unit -> unit Lwt.t;
    param    : A.param;
  }


  let bars : (int, t) Hashtbl.t = Hashtbl.create 128


  let make bar_id callback param =
    let promise, resolver = Lwt.wait () in
    let bar = { promise; resolver; callback; param } in
    Hashtbl.add bars bar_id bar


  let wait bar_id =
    let bar = Hashtbl.find bars bar_id in
    let%lwt () = bar.promise in
    let%lwt () = bar.callback () in
    Hashtbl.remove bars bar_id;
    Lwt.return ()


  let wakeup bar_id =
    let bar = Hashtbl.find bars bar_id in
    Lwt.wakeup bar.resolver ()


  let get bar_id = Hashtbl.find bars bar_id


  let mem bar_id = Hashtbl.mem bars bar_id

end
