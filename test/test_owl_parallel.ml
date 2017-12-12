(** [ Test Parallel Module in Owl ] *)

module Ctx = Actor_mapre

let test_naive () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  Ctx.load "default" |> Ctx.map (fun _ -> print_endline "hello") |> ignore;
  Ctx.terminate ()

(** [ Test parameter server ]  *)

(*
module PS = Actor_param

let retrieve_model () =
  let open Owl.Neural.S in
  let open Graph in
  (* model has been initialised *)
  try PS.get "model"
  (* model does not exist, init *)
  with Not_found -> (
    Actor_logger.warn "model does not exists, init now ...";
    let nn =
      input [|28;28;1|]
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
  Owl.Neural.S.Graph.print model; flush_all ();
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
    let open Owl.Neural.S in

    let x, _, y = Owl.Dataset.load_cifar_train_data 1 in

    let params = Params.config
    ~batch:(Batch.Sample 100) ~learning_rate:(Learning_Rate.Adagrad 0.005) 0.01 in
    Owl.Neural.S.Graph.train_cnn ~params model x y |> ignore;

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
*)

(* test parameter server engine *)
module M2 = Owl_neural_parallel.Make (Owl.Neural.S.Graph) (Actor_param)
let test_neural_parallel () =
  let open Owl.Neural.S in
  let open Graph in
  let nn =
    input [|32;32;3|]
    |> normalisation ~decay:0.9
    |> conv2d [|3;3;3;32|] [|1;1|] ~act_typ:Activation.Relu
    |> conv2d [|3;3;32;32|] [|1;1|] ~act_typ:Activation.Relu ~padding:VALID
    |> max_pool2d [|2;2|] [|2;2|] ~padding:VALID
    |> dropout 0.1
    |> conv2d [|3;3;32;64|] [|1;1|] ~act_typ:Activation.Relu
    |> conv2d [|3;3;64;64|] [|1;1|] ~act_typ:Activation.Relu ~padding:VALID
    |> max_pool2d [|2;2|] [|2;2|] ~padding:VALID
    |> dropout 0.1
    |> fully_connected 512 ~act_typ:Activation.Relu
    |> linear 10 ~act_typ:Activation.Softmax
    |> get_network
  in

  let x, _, y = Owl.Dataset.load_cifar_train_data 1 in
  (*
  let params = Params.config
    ~batch:(Batch.Mini 100) ~learning_rate:(Learning_Rate.Adagrad 0.002) 0.05 in
  *)
  let chkpt state =
    if Checkpoint.(state.current_batch mod 1 = 0) then (
      Checkpoint.(state.stop <- true);
      (* Log.info "sync model with server" *)
    )
  in

  let params = Params.config
    ~batch:(Batch.Sample 100) ~learning_rate:(Learning_Rate.Adagrad 0.001)
    ~checkpoint:(Checkpoint.Custom chkpt) ~stopping:(Stopping.Const 1e-6) 10.
  in
  let url = Actor_config.manager_addr in
  let jid = Sys.argv.(1) in
  M2.train ~params nn x y jid url


module M1 = Owl_parallel.Make_Distributed (Owl.Dense.Ndarray.D) (Actor_mapre)
let test_owl_distributed () =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  (* some tests ... *)
  let x = M1.uniform [|2;3;4|] in
  Owl.Dense.Ndarray.D.print (M1.to_ndarray x); flush_all ();

  let y = M1.init [|2;3;4|] float_of_int in
  let _ = M1.set y [|1;2;3|] 0. in
  let y = M1.add x y in
  Owl.Dense.Ndarray.D.print (M1.to_ndarray y); flush_all ();

  let a = M1.get y [|1;2;2|] in
  Actor_logger.info "get ===> %g" a;

  let x = M1.ones [|200;300;400|] in
  let x = M1.map (fun a -> a +. 1.) x in
  let a = M1.fold (+.) x 0. in
  let b = M1.sum x in
  Actor_logger.info "fold vs. sum ===> %g, %g" a b;

  Actor_logger.info "start retrieving big x";
  let x = M1.to_ndarray x in
  Actor_logger.info "finsh retrieving big x";
  Actor_logger.info "sum x = %g" (Owl.Arr.sum' x)


let _ = test_neural_parallel ()
