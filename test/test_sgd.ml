(** [ Test stochastic gradient descent ]  *)

open Owl

module MX = Dense.Real
module PS = Parameter

let datax = ref (MX.empty 0 0)
let datay = ref (MX.empty 0 0)
let model = ref (MX.empty 0 0)
let gradient = Owl_optimise.square_grad

let _print_model x =
  let s = MX.fold (fun s y -> s @ [string_of_float y]) [] x in
  String.concat " " s

(* prepare some synthetic data *)
let load_data () =
  let x = MX.uniform 1000 3 in
  let p = MX.of_array [|0.3;0.5;0.7|] 3 1 in
  (* let b = MX.gaussian ~sigma:0.05 1000 1 in *)
  let b = MX.zeros 1000 1 in
  let y = MX.((x $@ p) +@ b) in
  datax := x;
  datay := y

let load_model () =
  let p = MX.of_array [|0.1;0.1;0.1|] 3 1 in
  model := p

let _ = load_data (); load_model ()

let calc_gradient x y p b g =
  let x, i = MX.draw_rows x b in
  let y = MX.rows y i in
  let y' = MX.(x $@ p) in
  let d = g x y y' in
  Logger.debug "loss ==> %.10f" (Owl_optimise.square_loss y y' |> MX.sum);
  d

let schedule workers =
  Logger.debug "scheduling ...";
  let tasks = List.map (fun x ->
    let k = 0 in
    let v, _ = PS.get k in
    (x, [(k,v)])
  ) workers in
  Logger.debug "scheduling done ...";
  tasks

let push id vars =
  (* update model *)
  List.iter (fun (k,v) -> model := v) vars;
  Logger.debug "=== %s" (_print_model !model);
  let delta = List.map (fun (k,v) ->
    let d = calc_gradient !datax !datay !model 10 gradient in
    (k, d)
  ) vars
  in
  delta

let pull vars =
  List.map (fun (k,d) ->
    let v0, _ = PS.get k in
    let v1 = MX.(v0 -@ (d *$ 0.001)) in
    (k,v1)
  ) vars

(* start running distributed sgd *)
let run_sgd () =
  (* register schedule, push, pull functions *)
  PS.register_schedule schedule;
  PS.register_pull pull;
  PS.register_push push;
  (* prepare data and model *)
  let _ = Paramserver.set 0 !model 0 in
  (* start running the ps *)
  PS.start Sys.argv.(1) Config.manager_addr;
  Logger.info "do some work at master node";
  ()

let _ = run_sgd ()
