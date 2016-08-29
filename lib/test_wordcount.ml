(** []
  test the context module
*)

module Ctx = Context

let wordcount () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  let x = Ctx.map (fun x -> x) "wordcount" in
  Context.terminate ()

let _ = wordcount ()
