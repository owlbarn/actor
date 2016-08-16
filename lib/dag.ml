(** [ DAG module ]
  maintains a directed acyclic graph of computation.
*)

type t = {
  data_id : string;
  finshed : bool;
  parents : (string * string) list;
}

let _graph : t array = [||]
