(** []
  test the context module
*)

let print_float_array x =
  Array.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let test () =
  Context.init Sys.argv.(1) "tcp://localhost:5555";
  (* Test map *)
  let x1 = Context.map (fun v -> Array.map (fun x -> x *. 2.) v) "default" in
  List.iter (fun x -> print_float_array x) (Context.collect x1);
  (* Test broadcast *)
  let y = Context.broadcast 3. in
  let x2 = Context.map (fun v -> Array.map (fun x -> x +. (Context.get_value y)) v) x1 in
  List.iter (fun x -> print_float_array x) (Context.collect x2);
  (* Test union *)
  let x3 = Context.union x1 x2 in
  List.iter (fun x -> print_float_array x) (Context.collect x3);
  (* Terminate *)
  Context.terminate ()

let () = test ()
