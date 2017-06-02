(** [ Test stochastic gradient descent ]  *)

open Owl
module MX = Mat

let data_x = MX.uniform 1000 3
(*let data_y = let p = MX.of_array [|0.3;0.5;0.7|] 3 1 in MX.(data_x $@ p)
let model = MX.of_array [|0.1;0.1;0.1|] 3 1*)
let data_y = let p = MX.of_array [|0.3;0.5;0.7;0.4;0.9;0.2|] 3 2 in MX.(data_x *@ p)
let model = MX.of_array [|0.1;0.1;0.1;0.1;0.1;0.1|] 3 2
let gradfn = Owl_optimise.square_grad
let lossfn = Owl_optimise.square_loss

let _ =
  Actor_param_sgd.init data_x data_y model gradfn lossfn;
  Actor_param_sgd.start Sys.argv.(1)
