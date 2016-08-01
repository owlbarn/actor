(** []
  test the context module
*)

let data = Array.init 5 (fun x -> Random.float 10.)

let test () =
  Context.init "job_1980" "tcp://localhost:5555";
  Context.map (fun x -> x) "???"

let () = test ()
