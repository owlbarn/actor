(** [ Context ]
  maintain a context for each applicatoin
*)

type t = {
  manager : string;
}

let _context = ref {
  manager = "";
}

let init url =
  _context := { manager = url }

let map f = None

let reduce f = None

let collect f = None

let execute f = None
