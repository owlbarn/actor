(** []
  test the context module
*)

let print_float_array x =
  Array.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let test () =
  Context.init "job_1980" "tcp://localhost:5555";
  let x = Context.map (fun v -> Array.map (fun x -> x +. 1.) v) "default" in
  List.iter (fun x -> print_float_array x) (Context.collect x);
  let x = Context.map (fun v -> Array.map (fun x -> x *. 2.) v) x in
  List.iter (fun x -> print_float_array x) (Context.collect x);
  Context.terminate ()

let () = test ()
