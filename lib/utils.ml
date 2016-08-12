(** [
  Some shared helper functions
]  *)

let logger s =
  let open Unix in
  let t = gmtime (time ()) in
  Printf.printf "[#%i %02i:%02i:%02i] %s\n%!"
    (getpid ()) t.tm_hour t.tm_min t.tm_sec s
