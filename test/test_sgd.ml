(** [ Test coordinate descent ]  *)

open Owl
module MX = Dense.Real
module PS = Parameter

let param = Array.(init 1000 (fun x -> x) |> to_list)

let pivot = ref 0

let calc_gradient x y p b g =
  let x, i = MX.draw_rows x b in
  let y = MX.rows y i in
  let y' = MX.(x $@ p) in
  let d = g x y y' in
  d

let schedule workers =
  Logger.debug "scheduling ...";
  let tasks = List.map (fun x ->
    let k, v = !pivot, 0.5 in
    pivot := !pivot + 1;
    (x, [(k,v)])
  ) workers in
  Logger.debug "scheduling done ...";
  tasks

let push id vars =
  Logger.debug "worker node %s ..." id;
  vars

let pull vars =
  Logger.debug "master node ...";
  vars

let test_sgd () =
  PS.register_schedule schedule;
  PS.register_pull pull;
  PS.register_push push;
  PS.start Sys.argv.(1) Config.manager_addr;
  Logger.info "do some work at master node"

(* prepare some synthetic data *)
let load_data () =
  let x = MX.uniform 1000 3 in
  let p = MX.uniform 3 1 in
  let b = MX.gaussian ~sigma:0.05 1000 1 in
  let y = MX.((x $@ p) +@ b) in
  x, y

let load_model () = None


let _ = test_sgd ()
