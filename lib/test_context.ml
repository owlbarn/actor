(** []
  test the context module
*)

let test () =
  Context.init "liang" "tcp://localhost:5555"

let () = test ()
