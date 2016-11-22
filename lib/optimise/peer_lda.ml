(** [ Peer LDA ] Latent Dirichlet Allocation *)

open Owl
open Types

module MS = Sparse.Real
module MD = Dense.Real
module P2P = Peer

(* model variables *)

let n_d = ref 0
let n_k = ref 0
let n_v = ref 0

let alpha = ref 0.
let beta = ref 0.
let alpha_k = ref 0.
let beta_v = ref 0.

let t_dk = ref (MS.zeros 1 1)
let t_wk = ref (MS.zeros 1 1)
let t__k = ref (MD.zeros 1 1)
let t__z = ref [| [||] |]

let n_iter = 1_000
let data = ref [| [||] |]
let vocb : (string, int) Hashtbl.t ref = ref (Hashtbl.create 1)

let include_token w d k =
  MD.(set !t__k 0 k (get !t__k 0 k +. 1.));
  MS.(set !t_wk w k (get !t_wk w k +. 1.));
  MS.(set !t_dk d k (get !t_dk d k +. 1.))

let exclude_token w d k =
  MD.(set !t__k 0 k (get !t__k 0 k -. 1.));
  MS.(set !t_wk w k (get !t_wk w k -. 1.));
  MS.(set !t_dk d k (get !t_dk d k -. 1.))

(* init the model based on: topics, vocabulary, tokens *)
let init k v d =
  Log.info "init the model";
  data := d;
  vocb := v;
  (* set model parameters *)
  n_d  := Array.length d;
  n_v  := Hashtbl.length v;
  n_k  := k;
  t_dk := MS.zeros !n_d !n_k;
  t_wk := MS.zeros !n_v !n_k;
  t__k := MD.zeros 1 !n_k;
  (* set model hyper-parameters *)
  alpha := 50.;
  alpha_k := !alpha /. (float_of_int !n_k);
  beta := 0.1;
  beta_v := (float_of_int !n_v) *. !beta;
  (* randomise the topic assignment for each token *)
  t__z := Array.mapi (fun i s ->
    Array.init (Array.length s) (fun j ->
      let k' = Stats.Rnd.uniform_int ~a:0 ~b:(k - 1) () in
      include_token s.(j) i k';
      k'
    )
  ) d;
  (* init local model / kv store *)
  for i = 0 to !n_v do
    P2P.set i [||]
  done

let sampling d =
  let p = MD.zeros 1 !n_k in
  Array.iteri (fun i w ->
    let k = !t__z.(d).(i) in
    exclude_token w d k;
    (* make cdf function *)
    Logger.error "+++";
    let x = ref 0. in
    for j = 0 to !n_k - 1 do
      x := !x +. (MS.get !t_dk d j +. !alpha_k) *. (MS.get !t_wk w j +. !beta) /. (MD.get !t__k 0 j +. !beta_v);
      MD.set p 0 j !x;
    done;
    Logger.error "===";
    (* draw a sample *)
    let u = Stats.Rnd.uniform () *. !x in
    let k = ref 0 in
    while (MD.get p 0 !k) < u do Logger.error "--- %i %i %i" d i !k; k := !k + 1 done;
    include_token w d !k;
    !t__z.(d).(i) <- !k;
      Logger.error "***";
  ) !data.(d)

let schedule _context =
  Logger.info "schedule @ %s, step:%i" !_context.master_addr !_context.step;
  let d = Array.init !n_v (fun i -> i) in
  Stats.choose d (!n_v / 10) |> Array.to_list

let pull _context updates =
  Logger.info "pull @ %s, %i" !_context.myself_addr !_context.step;
  updates

let push _context params =
  Logger.info "push @ %s" !_context.master_addr;
  (* reset and re-assemble local model *)
  t_wk := MS.zeros !n_v !n_k;
  List.iter (fun (w,a) ->
    Array.iter (fun (k,c) -> MS.(set !t_wk w k c)) a
  ) params;
  (* iterate all local docs *)
  for j = 0 to !n_d - 1 do
    Logger.info "gibbs sampling #%i" j;
    sampling j
  done;
  [ ]

let barrier _context = Barrier.p2p_asp_local _context

let stop _context = !_context.step > 1_0

let start jid =
  (* register schedule, push, pull functions *)
  P2P.register_barrier barrier;
  P2P.register_schedule schedule;
  P2P.register_push push;
  P2P.register_pull pull;
  P2P.register_stop stop;
  (* start running the ps *)
  Logger.info "P2P: lda algorithm starts running ...";
  P2P.start jid Config.manager_addr
