(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


type operation =
  (* General messge types *)
  | Reg_Req
  | Reg_Rep
  | Heartbeat


type message = {
  mutable uuid      : string;
  mutable addr      : string;
  mutable operation : operation;
}


type param_context = {
  mutable myself  : string;
  mutable server  : string;
  mutable client  : string array;
  mutable book    : (string, string) Hashtbl.t;
  mutable waiting : (string, string) Hashtbl.t;
}


let encode_message uuid addr operation =
  let m = { uuid; addr; operation } in
  Marshal.to_string m []


let decode_message data : message = Marshal.from_string data 0
