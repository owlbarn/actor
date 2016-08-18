(** []
  test the context module
*)

let print_float_list x =
  List.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let test () =
  Context.init Sys.argv.(1) "tcp://localhost:5555";
  (* Test map *)
  let x1 = Context.map (fun x -> x *. 2.) "default" in
  (* Test broadcast *)
  let y = Context.broadcast 3. in
  let x2 = Context.map (fun x -> x +. (Context.get_value y)) x1 in
  (* Test union *)
  let x3 = Context.union x1 x2 in
  (* Test filter *)
  let x4 = Context.filter ((>) 10.) x3 in
  (* collect data *)
  List.iter (fun x -> print_float_list x) (Context.collect x1);
  List.iter (fun x -> print_float_list x) (Context.collect x2);
  List.iter (fun x -> print_float_list x) (Context.collect x3);
  List.iter (fun x -> print_float_list x) (Context.collect x4);
  print_endline ("total elements: " ^ (string_of_int (Context.count x4)));
  (* Terminate *)
  Context.terminate ()

let () = test ()
