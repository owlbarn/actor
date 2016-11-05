(** [ Distributed Stochastic Gradient Decendent ]
  Each row in the data matrix is a data point;
  each column in the model matrix is a classifier.
 *)

open Owl

module MX = Dense.Real
module PS = Parameter

(* variables used in distributed sgd *)
let data_x = ref (MX.empty 0 0)
let data_y = ref (MX.empty 0 0)
let _model = ref (MX.empty 0 0)
let gradfn = ref Owl_optimise.square_grad
let lossfn = ref Owl_optimise.square_loss
let step_t = ref 0.001

(* prepare data, model, gradient, loss *)
let init x y m g l =
  data_x := x;
  data_y := y;
  _model := m;
  gradfn := g;
  lossfn := l

let calculate_gradient b x y m g l =
  let xt, i = MX.draw_rows x b in
  let yt = MX.rows y i in
  let yt' = MX.(xt $@ m) in
  let d = g xt yt yt' in
  Logger.debug "loss = %.10f" (l yt yt' |> MX.sum);
  d

let schedule workers =
  let _, n = MX.shape !_model in
  List.map (fun x ->
    (* randomly choose a classifier *)
    let k = Stats.Rnd.uniform_int ~a:0 ~b:(n - 1) () in
    let v, _ = PS.get k in
    (x, [(k,v)])
  ) workers

let push id vars =
  (* update local model, need to improve for sparsity *)
  List.iter (fun (k,v) ->
    MX.copy_row_to v !_model k
  ) vars;
  (* compute the assigned work, return the update gradient *)
  List.map (fun (k,v) ->
    let d = calculate_gradient 10 !data_x !data_y !_model !gradfn !lossfn in
    let d = MX.(d *$ !step_t) in
    (k, d)
  ) vars

let pull vars =
  List.map (fun (k,d) ->
    let v0, _ = PS.get k in
    let v1 = MX.(v0 -@ d) in
    (k,v1)
  ) vars

let start () =
  (* register schedule, push, pull functions *)
  PS.register_schedule schedule;
  PS.register_pull pull;
  PS.register_push push;
  (* pre-cache the model in the server's kv store *)
  MX.iteri_rows (fun k v -> Paramserver.set k v) !_model;
  (* start running the ps *)
  PS.start Sys.argv.(1) Config.manager_addr;
  Logger.info "sdg start running ..."
