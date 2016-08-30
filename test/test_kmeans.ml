(** []
  test the context module
*)

let kmeans () =
  Context.init Sys.argv.(1) "tcp://localhost:5555";
  let x = ref "default" in
  for i = 0 to 999 do
    x := Context.map (fun x -> x +. 1.) !x;
  done;
  let _ = Context.collect !x in
  Context.terminate ()

let _ = kmeans ()
