(** []
  test the context module
*)

let data = Array.init 5 (fun x -> Random.float 10.)

let test () =
  Context.init "job_1980" "tcp://localhost:5555";
  let x = Context.map (fun v -> Array.map (fun x -> x +. 1.) v) "default" in
  let _ = Context.map (fun v -> Array.map (fun x -> x *. 2.) v) x in
  ()

let () = test ()
