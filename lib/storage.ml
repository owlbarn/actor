(** [ Storage ]
  provide a basic persistent storage service
*)

let unix_load x =
  let l = Unix.((stat x).st_size) in
  let b = Bytes.create l in
  let f = Unix.(openfile x [O_RDONLY] 0o644) in
  let _ = Unix.read f b 0 l in b

let unix_save x b =
  let f = Unix.(openfile x [O_WRONLY; O_CREAT] 0o644) in
  let l = Bytes.length b in
  Unix.write f b 0 l

let hdfs_load x = None

let hdfs_save x = None

let irmin_load x = None

let irmin_save x = None
