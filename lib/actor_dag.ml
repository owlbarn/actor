(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

open Actor_types

type vlabel = { c : color; f : string }

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

let _vlabel = Hashtbl.create 1048576

let get_vlabel_f x = (Hashtbl.find _vlabel x).f

let add_edge f u v c =
  if (Hashtbl.mem _vlabel u) = false then
    Hashtbl.add _vlabel u { c = Green; f = "" };
  Hashtbl.add _vlabel v { c = c; f = f };
  Digraph.add_edge !_graph u v

let stages_eager () =
  let r, s = ref [], ref [] in
  let _ = TopoOrd.iter (fun v ->
    match (Hashtbl.find _vlabel v).c with
    | Blue  -> s := !s @ [v]; r := !r @ [!s]; s := []
    | Red   -> s := !s @ [v]
    | Green -> ()
  ) !_graph in
  if List.length !s = 0 then !r else !r @ [!s]

let rec _stages_lazy v =
  let l = ref [v] in
  List.iter (fun u ->
    match (Hashtbl.find _vlabel u).c with
    | Blue | Red -> l := (_stages_lazy u) @ !l
    | Green -> ()
  ) (Digraph.pred !_graph v); !l

let stages_lazy v =
  let r, s = ref [], ref [] in
  let _ = List.iter (fun v ->
    match (Hashtbl.find _vlabel v).c with
    | Blue  -> s := !s @ [v]; r := !r @ [!s]; s := []
    | Red   -> s := !s @ [v]
    | Green -> ()
  ) (_stages_lazy v) in
  if List.length !s = 0 then !r else !r @ [!s]

let mark_stage_done s =
  List.iter (fun k ->
    let v = Hashtbl.find _vlabel k in
    Hashtbl.add _vlabel k { c = Green; f = v.f }
  ) s

(* FIXME: the following functions are for debugging *)

let print_vertex v =
  let x = Hashtbl.find _vlabel v in
  match x.c with
  | Red   -> Printf.printf "(%s, Red); " v
  | Green -> Printf.printf "(%s, Green); " v
  | Blue  -> Printf.printf "(%s, Blue); " v

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
  add_edge "" "1" "4" Blue;
  add_edge "" "1" "6" Red;
  add_edge "" "2" "3" Red;
  add_edge "" "4" "3" Red;
  add_edge "" "3" "5" Blue;
  add_edge "" "4" "7" Red;
  add_edge "" "6" "8" Blue;
  add_edge "" "7" "8" Blue;
  TopoOrd.iter (fun v -> print_vertex v) !_graph;
  print_stages (stages_lazy "5");
