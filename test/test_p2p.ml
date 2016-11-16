(** [ Test parameter server ]  *)

open Owl

module P2P = Peer
module MX = Dense.Real

let data_x = MX.uniform 1000 3
let data_y = let p = MX.of_array [|0.3;0.5;0.7;0.4;0.9;0.2|] 3 2 in MX.(data_x $@ p)
let model = MX.of_array [|0.1;0.1;0.1;0.1;0.1;0.1|] 3 2

let gradfn = ref Owl_optimise.square_grad
let lossfn = ref Owl_optimise.square_loss
let step_t = ref 0.001

let show_info () =
  let v0, _ = P2P.get 0 in
  let v1, _ = P2P.get 1 in
  let m = MX.(v0 @|| v1) in
  Logger.info "%f %f" m.{0,0} m.{0,1};
  Logger.info "%f %f" m.{1,0} m.{1,1};
  Logger.info "%f %f" m.{2,0} m.{2,1}

let init_model () = MX.iteri_cols (fun i v -> P2P.set i v) model

let calculate_gradient b x y m g l =
  let xt, i = MX.draw_rows x b in
  let yt = MX.rows y i in
  let yt' = MX.(xt $@ m) in
  let d = g xt yt yt' in
  Logger.debug "loss = %.10f" (l yt yt' |> MX.sum);
  d

let schedule id =
  show_info ();
  Logger.debug "%s: scheduling ..." id;
  let n = MX.col_num model in
  let k = Stats.Rnd.uniform_int ~a:0 ~b:(n - 1) () in
  let v, _ = P2P.get k in
  [ (k, v) ]

let push id params =
  List.map (fun (k,v) ->
    Logger.debug "%s: working on %i ..." id k;
    let y = MX.col data_y k in
    let d = calculate_gradient 10 data_x y v !gradfn !lossfn in
    let d = MX.(d *$ !step_t) in
    (k, d)
  ) params

let barrier updates =
  Logger.debug "checking barrier ...";
  true

let pull updates =
  Logger.debug "pulling updates ...";
  List.map (fun o ->
    let _, k, v, t = Obj.obj o in
    let v0, _ = P2P.get k in
    let v1 = MX.(v0 -@ v) in
    k, v1, t
  ) updates

let test_context () =
  P2P.register_barrier barrier;
  P2P.register_pull pull;
  P2P.register_schedule schedule;
  P2P.register_push push;
  init_model ();
  P2P.start Sys.argv.(1) Config.manager_addr

let _ = test_context ()
