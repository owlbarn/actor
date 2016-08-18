(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

open Types

type vlabel = { data : string; color : color; }

module Digraph = struct
  module V' = struct
    type t = string
    let hash = Hashtbl.hash
    let equal = (=)
    let compare = Pervasives.compare
  end
  module E' = struct
    type t = string
    let compare = Pervasives.compare
    let default = ""
  end
  include Graph.Imperative.Digraph.ConcreteLabeled (V') (E')
end

module TopoOrd = Graph.Topological.Make_stable (Digraph)

let _graph = ref (Digraph.create ())

let _vlabel : vlabel StrMap.t ref = ref StrMap.empty

let add_edge f x y c = None

let print_tasks () = ()
