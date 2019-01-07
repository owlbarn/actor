(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2019 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

type color = Red | Green | Blue


type message_type =
  (* General messge types *)
  | Reg_Req of string * string
  | Reg_Rep of string
  | Heartbeat  of string * string


type mapre_config = {
  mutable myself  : string;
  mutable server  : string;
  mutable client  : string array;
  mutable book    : (string, string) Hashtbl.t;
  mutable waiting : (string, string) Hashtbl.t;
}


type mapre_message = {
  mutable uuid : string;
  mutable addr : string;
  mutable data : string;
}


type message_rec = {
  mutable bar : int;
  mutable typ : message_type;
  mutable par : string array;
}



  (** two functions to translate between message rec and string *)

  let to_msg b t p =
    let m = { bar = b; typ = t; par = p } in
    Marshal.to_string m [ ]


  let of_msg s =
    let m : message_type = Marshal.from_string s 0 in
    m
