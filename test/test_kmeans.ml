(** [ Naive K-means implementation ]
*)

module Ctx = Context

let kmeans () =
  let x = ref "default" in
  for i = 0 to 99 do
    x := Ctx.map (fun x -> x +. 1.) !x;
  done;
  let _ = Ctx.collect !x in ()


let _ =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  kmeans ();
  Ctx.terminate ()
