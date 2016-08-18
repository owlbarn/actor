(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

open Types

type vlabel = { color : color; f : string }

module Digraph = struct
  module V' = struct
    type t = string
    let hash = Hashtbl.hash
    let equal = (=)
    let compare = Pervasives.compare
  end
  module E' = struct
    type t = int
    let compare = Pervasives.compare
    let default = 0
  end
  include Graph.Imperative.Digraph.ConcreteLabeled (V') (E')
end

module TopoOrd = Graph.Topological.Make_stable (Digraph)

let _graph = ref (Digraph.create ())

let _vlabel : vlabel StrMap.t ref = ref StrMap.empty

let add_edge f u v c =
  if (StrMap.mem u !_vlabel) = false then
    _vlabel := StrMap.add u { color = Green; f = "" } !_vlabel;
  _vlabel := StrMap.add v { color = c; f = f } !_vlabel;
  Digraph.add_edge !_graph u v

let stages () =
  let r, s = ref [], ref [] in
  let _ = TopoOrd.iter (fun v ->
    match (StrMap.find v !_vlabel).color with
    | Blue -> ( s := !s @ [v]; r := !r @ [!s]; s := [] )
    | Red -> s := !s @ [v]
    | Green -> ()
  ) !_graph in
  if List.length !s = 0 then !r else !r @ [!s]

let mark_stage_done s =
  List.iter (fun k ->
    let v = StrMap.find k !_vlabel in
    _vlabel := StrMap.add k { color = Green; f = v.f } !_vlabel
  ) s

let print_vertex v =
  let x = StrMap.find v !_vlabel in
  match x.color with
  | Red -> Printf.printf "(%s, Red); " v
  | Green -> Printf.printf "(%s, Green); " v
  | Blue -> Printf.printf "(%s, Blue); " v

let print_stages x =
  print_endline "";
  List.iter (fun l ->
    print_string "stage: ";
    List.iter (fun v ->
      print_vertex v
    ) l; print_endline ""
  ) x

let print_tasks () = TopoOrd.iter (fun v -> print_vertex v) !_graph

(*
let test () =
  add_edge "" "1" "2" Red;
  add_edge "" "1" "4" Red;
  add_edge "" "1" "6" Red;
  add_edge "" "2" "3" Blue;
  add_edge "" "4" "3" Blue;
  add_edge "" "3" "5" Blue;
  add_edge "" "4" "7" Red;
  add_edge "" "6" "8" Blue;
  add_edge "" "7" "8" Blue;
  TopoOrd.iter (fun v -> print_vertex v) !_graph;
  print_stages (stages ());
  mark_stage_done (List.nth (stages ()) 0);
  TopoOrd.iter (fun v -> print_vertex v) !_graph

let _ = test ()
*)
