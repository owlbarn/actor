(** []
  test the context module
*)

let data = Array.init 5 (fun x -> Random.float 10.)

let print_float_array x =
  Array.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let test () =
  Context.init "job_1980" "tcp://localhost:5555";
  let x = Context.map (fun v -> Array.map (fun x -> x +. 1.) v) "default" in
  let x = Context.map (fun v -> Array.map (fun x -> x *. 2.) v) x in
  let y = Context.collect x in
  List.iter (fun x -> print_float_array x) y;
  ()

let () = test ()
