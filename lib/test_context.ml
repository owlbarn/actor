(** []
  test the context module
*)

let test () =
  Context.init "job_1980" "tcp://localhost:5555"

let () = test ()
