(** [ Logger module ]  *)

include Owl_log

(* update logging config *)
let update_config level logdir fname =
  set_level level;
  match fname with
  | "" -> set_color true;  set_output stderr
  | _  -> set_color false; open_out (logdir^fname) |> set_output
