(** []
  test the context module
*)

module Ctx = Context

let print_result x = List.iter (fun (k,v) -> Printf.printf "%s : %i\n" k v) x

let stop_words = ["a";"are";"is";"in";"it";"that";"this";"and";"to";"of";"so";
  "will";"can";"which";"for";"on";"in";"an";"with";"the";"-"]

let wordcount () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  let _ = "wordcount.data"
  |> Ctx.map Str.(split (regexp "[ \t\n]"))
  |> Ctx.flatten
  |> Ctx.map String.lowercase
  |> Ctx.filter (fun x -> (String.length x) > 0)
  |> Ctx.filter (fun x -> not (List.mem x stop_words))
  |> Ctx.map (fun k -> (k,1))
  |> Ctx.reduce_by_key (+)
  |> Ctx.collect
  |> List.flatten |> print_result in
  Context.terminate ()

let _ = wordcount ()
