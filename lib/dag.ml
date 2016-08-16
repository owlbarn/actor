(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

type color = Red | Green | Blue

module V = struct
  type t = { data_id : string; color : color; }
  let hash x = Hashtbl.hash x.data_id
  let equal x y = x.data_id = y.data_id
  let compare x y = Pervasives.compare x.data_id y.data_id

end

module E = struct
  type t = float
  let compare = Pervasives.compare
  let default = 0.0
end

module Graph = Graph.Imperative.Graph.ConcreteLabeled(V)(E)

let _graph = None
