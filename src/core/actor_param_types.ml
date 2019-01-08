(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)


type operation =
  (* General messge types *)
  | Reg_Req
  | Reg_Rep
  | Heartbeat
  | Exit
  (* Model parallel types *)
  | PS_Get
  | PS_Set
  | PS_Schd of string
  | PS_Push of string


type message = {
  mutable uuid      : string;
  mutable addr      : string;
  mutable operation : operation;
}


type param_context = {
  mutable my_uuid     : string;
  mutable my_addr     : string;
  mutable server_uuid : string;
  mutable server_addr : string;
  mutable book        : Actor_book.t;
  mutable schedule    : string array -> (string * string) array;
  mutable push        : string -> string;
}


let encode_message uuid addr operation =
  let m = { uuid; addr; operation } in
  Marshal.to_string m []


let decode_message data : message = Marshal.from_string data 0
