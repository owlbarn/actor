(** [
  Some shared helper functions
]  *)

let logger s =
  let open Unix in
  let t = gmtime (Unix.time ()) in
  Printf.printf "[%02i:%02i:%02i] %s\n%!" t.tm_hour t.tm_min t.tm_sec s
