(** [ Test Parallel Module in Owl ] *)

module Ctx = Actor_mapre

module M = Owl_parallel.Make (Ctx)

let test_naive () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  Ctx.load "default" |> Ctx.map (fun _ -> print_endline "hello") |> ignore;
  Ctx.terminate ()

(** [ Test parameter server ]  *)

module PS = Actor_param

let retrieve_model () =
  let open Owl_neural in
  let open Owl_neural_feedforward in
  (* model has been initialised *)
  try PS.get "model"
  (* model does not exist, init *)
  with Not_found -> (
    Actor_logger.warn "model does not exists, init now ...";
    let nn = input [|28;28;1|]
      |> conv2d [|5;5;1;32|] [|1;1|] ~act_typ:Activation.Relu
      |> max_pool2d [|2;2|] [|2;2|]
      |> conv2d [|5;5;32;64|] [|1;1|] ~act_typ:Activation.Relu
      |> max_pool2d [|2;2|] [|2;2|]
      |> dropout 0.1
      |> fully_connected 1024 ~act_typ:Activation.Relu
      |> linear 10 ~act_typ:Activation.Softmax
    in
    PS.set "model" nn;
    PS.get "model"
  )


let schedule workers =
  let model = retrieve_model () |> fst in
  Owl_neural.Feedforward.print model; flush_all ();
  let tasks = List.map (fun x ->
    (x, [("model", model)])
  ) workers in tasks


let push id vars =
  let updates = List.map (fun (k, model) ->
    (*
    let sleep_t = Owl.Stats.Rnd.uniform_int ~a:1 ~b:10 () in
    Actor_logger.info "working %is" sleep_t;
    Owl_neural.Feedforward.print model; flush_all ();
    Unix.sleep sleep_t;
    *)
    let open Owl in
    let open Owl_neural in

    let x, _, y = Dataset.load_mnist_train_data () in
    let m = Dense.Matrix.S.row_num x in
    let x = Dense.Matrix.S.to_ndarray x in
    let x = Dense.Ndarray.S.reshape x [|m;28;28;1|] in

    let params = Params.config
    ~batch:(Batch.Mini 100) ~learning_rate:(Learning_Rate.Adagrad 0.005) 1. in
    Feedforward.train_cnn ~params model x y |> ignore;

    (k, model) ) vars in
  updates


let pull vars =
  List.map (fun (k,d) ->
    (*
    let v0, _ = PS.get k in
    let v1 = MX.(v0 - d) in
    *)
    let v1 = d in
    (k,v1)
  ) vars


let test_param () =
  PS.register_schedule schedule;
  PS.register_push push;
  PS.register_barrier Actor_barrier.param_asp;

  PS.start Sys.argv.(1) Actor_config.manager_addr;
  Actor_logger.info "do some work at master node"

(* test parameter server engine *)
module M2 = Owl_neural_parallel.Make (Actor_param) (Owl_neural_feedforward)
let test () =
  let open Owl in
  let open Owl_neural in
  let open Owl_neural_feedforward in
  let nn = input [|28;28;1|]
    |> conv2d [|5;5;1;32|] [|1;1|] ~act_typ:Activation.Relu
    |> max_pool2d [|2;2|] [|2;2|]
    |> conv2d [|5;5;32;64|] [|1;1|] ~act_typ:Activation.Relu
    |> max_pool2d [|2;2|] [|2;2|]
    |> dropout 0.1
    |> fully_connected 1024 ~act_typ:Activation.Relu
    |> linear 10 ~act_typ:Activation.Softmax
  in

  let x, _, y = Dataset.load_mnist_train_data () in
  let m = Dense.Matrix.S.row_num x in
  let x = Dense.Matrix.S.to_ndarray x in
  let x = Dense.Ndarray.S.reshape x [|m;28;28;1|] in

  (*
  let params = Params.config
    ~batch:(Batch.Mini 100) ~learning_rate:(Learning_Rate.Adagrad 0.002) 0.05 in
  *)

  let params = Params.config
    ~batch:(Batch.Mini 100) ~learning_rate:(Learning_Rate.Const 0.01) 0.05 in

  let url = Actor_config.manager_addr in
  let jid = Sys.argv.(1) in
  M2.train_cnn ~params nn x y jid url


let _ = test ()
