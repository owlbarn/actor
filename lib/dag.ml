(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

open Types

type vertex = { data : string; color : color; }

module Digraph = struct
  module V' = struct
    type t = vertex
    let hash x = Hashtbl.hash x.data
    let equal x y = x.data = y.data
    let compare x y = Pervasives.compare x.data y.data
  end
  module E' = struct
    type t = string
    let compare = Pervasives.compare
    let default = ""
  end
  include Graph.Imperative.Digraph.ConcreteLabeled (V') (E')
end

(* module Digraph = Graph.Imperative.Digraph.ConcreteLabeled (V) (E) *)
module TopoOrd = Graph.Topological.Make_stable (Digraph)

let _graph = ref (Digraph.create ())

let _get_vertex_color g x =
  let c = ref Green in
  let _ = Digraph.iter_vertex (fun v ->
    if v.data = x then c := v.color
  ) g in !c

let _set_vertex_color g x c =
  let u = { data = x; color = c } in
  let g = Digraph.map_vertex (fun v ->
    if v.data = x then u else v
  ) !_graph in _graph := g

let add_edge f x y c =
  let d = _get_vertex_color !_graph x in
  let x = { data = x; color = d; } in
  let y = { data = y; color = c; } in
  Digraph.add_edge_e !_graph (x, f, y)

let stages () =
  let r, s = ref [], ref [] in
  let _ = TopoOrd.iter (fun v ->
    match v.color with
    | Yellow | Blue -> (
      s := !s @ [v];
      r := !r @ [!s];
      s := [] )
    | Red -> s := !s @ [v]
    | Green -> ()
  ) !_graph in !r

let mark_stage_done s =
  List.iter (fun v -> _set_vertex_color !_graph v.data Green) s

let print_vertex v =
    match v.color with
    | Red -> Printf.printf "(%s, Red); " v.data
    | Green -> Printf.printf "(%s, Green); " v.data
    | Blue -> Printf.printf "(%s, Blue); " v.data
    | Yellow -> Printf.printf "(%s, Yellow); " v.data

let print_stages x =
  print_endline "";
  List.iter (fun l ->
    print_string "stage: ";
    List.iter (fun v ->
      print_vertex v
    ) l; print_endline ""
  ) x

let print_tasks () = TopoOrd.iter (fun v -> print_vertex v) !_graph

let test () =
  add_edge "" "1" "2" Red;
  add_edge "" "1" "4" Red;
  add_edge "" "1" "6" Red;
  add_edge "" "2" "3" Yellow;
  add_edge "" "4" "3" Yellow;
  add_edge "" "3" "5" Blue;
  add_edge "" "4" "7" Red;
  add_edge "" "6" "8" Blue;
  add_edge "" "7" "8" Blue;
  TopoOrd.iter (fun v -> print_vertex v) !_graph;
  print_stages (stages ());
  mark_stage_done (List.nth (stages ()) 0);
  TopoOrd.iter (fun v -> print_vertex v) !_graph

let _ = test ()
