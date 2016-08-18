(** []
  test the context module
*)

let print_float_array x =
  Array.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let test () =
  Context.init Sys.argv.(1) "tcp://localhost:5555";
  (* Test map *)
  let x = Context.map (fun v -> Array.map (fun x -> x *. 2.) v) "default" in
  List.iter (fun x -> print_float_array x) (Context.collect x);
  (* Test broadcast *)
  let y = Context.broadcast 3. in
  let x = Context.map (fun v -> Array.map (fun x -> x +. (Context.get_value y)) v) x in
  List.iter (fun x -> print_float_array x) (Context.collect x);
  (* Terminate *)
  Context.terminate ()

let () = test ()
