(** []
  test the context module
*)

module Ctx = Actor_mapre

let print_result x = List.iter (fun (k,v) -> Printf.printf "%s : %i\n" k v) x

let stop_words = ["a";"are";"is";"in";"it";"that";"this";"and";"to";"of";"so";
  "will";"can";"which";"for";"on";"in";"an";"with";"the";"-"]

let wordcount () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  Ctx.load "unix://data/wordcount.data"
  |> Ctx.flatmap Str.(split (regexp "[ \t\n]"))
  |> Ctx.map String.lowercase_ascii
  |> Ctx.filter (fun x -> (String.length x) > 0)
  |> Ctx.filter (fun x -> not (List.mem x stop_words))
  |> Ctx.map (fun k -> (k,1))
  |> Ctx.reduce_by_key (+)
  |> Ctx.collect
  |> List.flatten |> print_result;
  Ctx.terminate ()

let _ = wordcount ()
