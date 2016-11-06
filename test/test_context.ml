(** []
  test the context module
*)

let print_float_list x =
  List.iter (fun y -> Printf.printf "%.2f\t" y) x;
  print_endline ""

let print_kv_list x =
  List.iter (fun l ->
    if (List.length l) > 0 then
      ( List.iter (fun (k,v) -> Printf.printf "(%c,%.2f)\t" k v) l; print_endline "" )
  ) x

let print_join_list x =
  List.iter (fun l ->
    if (List.length l) > 0 then
      ( List.iter (fun (k,l1) ->
        Printf.printf "%c : " k;
        List.iter (fun v -> Printf.printf "%.2f; " v) l1 ) l;
        print_endline "" )
  ) x

let test () =
  Mapre.init Sys.argv.(1) "tcp://localhost:5555";
  (* Test map *)
  let x1 = Mapre.map (fun x -> x *. 2.) "default" in
  (* Test broadcast *)
  let y = Mapre.broadcast 3. in
  let x2 = Mapre.map (fun x -> x +. (Mapre.get_value y)) x1 in
  (* Test union *)
  let x3 = Mapre.union x1 x2 in
  (* Test filter *)
  let x4 = Mapre.filter ((>) 10.) x3 in
  (* Test shuffle & reduce_by_key *)
  let x5 = Mapre.map (fun x -> if x > 10. then ('a',x) else ('b',x)) x3 in
  let x7 = Mapre.reduce_by_key (max) x5 in
  let _  = (* print_kv_list *) (Mapre.collect x5) in
  (* collect data *)
  let _  = (* List.iter (fun x -> print_float_list x) *) (Mapre.collect x3) in
  Printf.printf "num of x4 is %s\n" (Mapre.count x4 |> string_of_int);
  Printf.printf "sum of x3 is %.2f\n" (Mapre.fold (+.) 0. x3);
  Printf.printf "max of x3 is %.2f\n" (Mapre.fold max 0. x3);
  print_kv_list (Mapre.collect x7);
  (* test join *)
  (* let x8 = Mapre.join x5 x5 in
  print_join_list (Mapre.collect x8); *)
  (* Terminate *)
  Mapre.terminate ()

let _ = test ()
