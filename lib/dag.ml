(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

open Graph.Imperative

type t = {
  data_id : string;
  finshed : bool;
  parents : (string * string) list;
}

module V = struct
  type t = string
  let compare = Pervasives.compare
  let hash = Hashtbl.hash
  let equal = (=)
end
module E = struct
  type t = float
  let compare = Pervasives.compare
  let default = 0.0
end

let _graph : t array = [||]
