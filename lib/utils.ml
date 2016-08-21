(** [
  Some shared helper functions
]  *)

open Types

let logger s =
  let open Unix in
  let t = gmtime (time ()) in
  Printf.printf "[#%i %02i:%02i:%02i] %s\n%!"
    (getpid ()) t.tm_hour t.tm_min t.tm_sec s

let group_by_key x =
  let d = ref StrMap.empty in
  List.iter (fun (k,v) ->
    let k = Marshal.to_string k [] in
    let u = if StrMap.mem k !d then StrMap.find k !d else [] in
    d := StrMap.add k (u @ [(k,v)]) !d ) x;
  StrMap.bindings !d
