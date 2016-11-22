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
  ) d

let sampling d =
  let p = MD.zeros 1 !n_k in
  Array.iteri (fun i w ->
    let k = !t__z.(d).(i) in
    exclude_token w d k;
    (* make cdf function *)
    let x = ref 0. in
    for j = 0 to !n_k - 1 do
      x := !x +. (MS.get !t_dk d j +. !alpha_k) *. (MS.get !t_wk w j +. !beta) /. (MD.get !t__k 0 j +. !beta_v);
      MD.set p 0 j !x;
    done;
    (* draw a sample *)
    let u = Stats.Rnd.uniform () *. !x in
    let k = ref 0 in
    while (MD.get p 0 !k) < u do k := !k + 1 done;
    include_token w d !k;
    !t__z.(d).(i) <- !k;
  ) !data.(d)

let schedule _context =
  let d = Array.make !n_v (fun i -> i) in
  Stats.choose d (!n_v / 10) |> Array.to_list

let pull updates = []

let push _context params =
  for j = 0 to !n_d - 1 do
    sampling j
  done

let barrier _context = Barrier.p2p_bsp _context

let stop _context = !_context.step > 1_000
