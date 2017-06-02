(** [ Service ]
  defines basic functionality of services
*)

open Actor_types

let _services = ref StrMap.empty

let mem id = StrMap.mem id !_services

let add id master =
  let s = { id = id; master = master; worker = [||] } in
  _services := StrMap.add id s !_services

let add_worker id wid =
  let service = StrMap.find id !_services in
  let workers = Array.append service.worker [| wid |] in
  service.worker <- workers

let find id = StrMap.find id !_services

let choose_workers id n =
  let s = StrMap.find id !_services in
  match Array.length s.worker > n with
  | true  -> Owl.Stats.choose s.worker n
  | false -> s.worker
