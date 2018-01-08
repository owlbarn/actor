(** [ Distributed Stochastic Gradient Descendent ] *)

open Owl
open Actor_types

module MX = Mat
module P2P = Actor_peer

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
  lossfn := l;
  MX.iteri_cols (fun i v -> P2P.set i v) !_model

let calculate_gradient b x y m g l =
  let xt, i = MX.draw_rows x b in
  let yt = MX.rows y i in
  let yt' = MX.(xt *@ m) in
  let d = g xt yt yt' in
  Actor_logger.debug "loss = %.10f" (l yt yt' |> MX.sum);
  d

let schedule _context =
  Actor_logger.debug "%s: scheduling ..." !_context.master_addr;
  let n = MX.col_num !_model in
  let k = Stats.Rnd.uniform_int ~a:0 ~b:(n - 1) () in
  [ k ]

let push _context params =
  List.map (fun (k,v) ->
    Actor_logger.debug "%s: working on %i ..." !_context.master_addr k;
    let y = MX.col !data_y k in
    let d = calculate_gradient 10 !data_x y v !gradfn !lossfn in
    let d = MX.(d *$ !step_t) in
    (k, d)
  ) params

let barrier _context =
  Actor_logger.debug "checking barrier ...";
  true

let pull _context updates =
  Actor_logger.debug "pulling updates ...";
  List.map (fun (k,v,t) ->
    let v0, _ = P2P.get k in
    let v1 = MX.(v0 - v) in
    k, v1, t
  ) updates

let stop _context = false

let start jid =
  (* register schedule, push, pull functions *)
  P2P.register_barrier barrier;
  P2P.register_schedule schedule;
  P2P.register_push push;
  P2P.register_pull pull;
  P2P.register_stop stop;
  (* start running the ps *)
  Actor_logger.info "P2P: sdg algorithm starts running ...";
  P2P.start jid Actor_config.manager_addr
