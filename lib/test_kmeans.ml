(** []
  test the context module
*)

let kmeans () =
  Context.init Sys.argv.(1) "tcp://localhost:5555";
  let x = ref "default" in
  for i = 0 to 500 do
    x := Context.map (fun x -> x +. 1.) !x;
  done;
  Context.collect !x;
  (* Terminate *)
  Context.terminate ()

let _ = kmeans ()
