(** [ Logger module ]  *)

include Log

(* update logging config *)
let update_config level logdir fname =
  set_log_level level;
  match fname with
  | "" -> color_on (); set_output stderr
  | _  -> color_off (); open_out (logdir^fname) |> set_output
