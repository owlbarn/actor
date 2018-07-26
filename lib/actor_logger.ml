(*
 * Actor - Parallel & Distributed Engine of Owl System
 * Copyright (c) 2016-2018 Liang Wang <liang.wang@cl.cam.ac.uk>
 *)

include Owl_log

(* update logging config *)
let update_config level logdir fname =
  set_level level;
  match fname with
  | "" -> set_color true;  set_output stderr
  | _  -> set_color false; open_out (logdir^fname) |> set_output
