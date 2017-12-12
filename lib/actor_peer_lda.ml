(** [ Peer LDA ] Latent Dirichlet Allocation *)

open Owl
open Actor_types

module MS = Sparse.Dok_matrix
module MD = Mat
module P2P = Actor_peer

(* local model hyper-parameters *)
let alpha = ref 0.
let beta = ref 0.
let alpha_k = ref 0.
let beta_v = ref 0.
(* local and global model variables *)
let t_dk = ref (MD.zeros 1 1)
let t_wk = ref (MD.zeros 1 1)
let t__k = ref (MD.zeros 1 1)
let t__z = ref [| [||] |]
(* data set variables *)
let n_d = ref 0
let n_k = ref 0
let n_v = ref 0
let data = ref [| [||] |]
let vocb : (string, int) Hashtbl.t ref = ref (Hashtbl.create 1)
let b__m = ref [||]   (* track if local model has been merged *)

let include_token w d k =
  MD.(set !t__k 0 k (get !t__k 0 k +. 1.));
  MD.(set !t_wk w k (get !t_wk w k +. 1.));
  MD.(set !t_dk d k (get !t_dk d k +. 1.))

let exclude_token w d k =
  MD.(set !t__k 0 k (get !t__k 0 k -. 1.));
  MD.(set !t_wk w k (get !t_wk w k -. 1.));
  MD.(set !t_dk d k (get !t_dk d k -. 1.))

let init k v d =
  Log.info "init the model";
  data := d;
  vocb := v;
  b__m := Array.make (Hashtbl.length v) false;
  (* set model parameters *)
  n_d  := Array.length d;
  n_v  := Hashtbl.length v;
  n_k  := k;
  t_dk := MD.zeros !n_d !n_k;
  t_wk := MD.zeros !n_v !n_k;
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
  for w = 0 to !n_v - 1 do P2P.set w [||] done;
  P2P.set (-1) !t__k

let rebuild_local_model () =
  Actor_logger.warn "rebuild local model start";
  t_wk := MD.zeros !n_v !n_k;
  Array.iteri (fun i d ->
    Array.iteri (fun j k ->
      let w = !data.(i).(j) in
      MD.(set !t_wk w k (get !t_wk w k +. 1.));
    ) d
  ) !t__z;
  Actor_logger.warn "rebuild local model finished"

let show_stats () =
  Actor_logger.info "t_wk = %.4f, t_dk = %.4f" (MD.density !t_wk) (MD.density !t_dk)

let sampling d h =
  let p = MD.zeros 1 !n_k in
  Array.iteri (fun i w ->
    if h.(w) = true then (
      let k = !t__z.(d).(i) in
      exclude_token w d k;
      (* make cdf function *)
      let x = ref 0. in
      for j = 0 to !n_k - 1 do
        x := !x +. (MD.get !t_dk d j +. !alpha_k) *. (MD.get !t_wk w j +. !beta) /. (MD.get !t__k 0 j +. !beta_v);
        MD.set p 0 j !x
      done;
      (* draw a sample *)
      let u = Stats.Rnd.uniform () *. !x in
      let k = ref 0 in
      while (MD.get p 0 !k) < u do k := !k + 1; done;
      include_token w d !k;
      !t__z.(d).(i) <- !k;
    )
  ) !data.(d)

let schedule _context =
  Actor_logger.info "schedule @ %s, step:%i" !_context.master_addr !_context.step;
  let d = Array.init !n_v (fun i -> i) in
  Stats.choose d (!n_v / 10) |> Array.to_list

let pull _context updates =
  let num_updates = List.fold_right (fun (_,a,_) x -> Array.length a + x) updates 0 in
  Actor_logger.info "pull @ %s, updates:%i" !_context.myself_addr num_updates;
  (* update t__k *)
  let tk_updates = List.filter (fun (w,_,_) -> w = -1) updates in
  if List.length tk_updates > 0 then (
    let t_k', _ = P2P.get (-1) in
    Actor_logger.error "%s ==> %i %f" !_context.myself_addr (List.length tk_updates) (MD.sum' t_k');
    List.iter (fun (_,a,t) ->
      Array.iter (fun (k,c) -> MD.(set t_k' 0 k (get t_k' 0 k +. c))) a
    ) tk_updates;
    P2P.set (-1) t_k'
  );
  (* update t_wk *)
  let wk_updates = List.filter (fun (w,_,_) -> w > -1) updates in
  let h = Hashtbl.create 256 in
  List.iter (fun (w,a,t) ->
    if Hashtbl.mem h w = false then (
      let r = MD.zeros 1 !n_k in
      let a', t' = P2P.get w in
      Array.iter (fun (k,c) -> MD.set r 0 k c) a';
      Hashtbl.add h w (r,t')
    );
    let r, t' = Hashtbl.find h w in
    Array.iter (fun (k,c) -> MD.(set r 0 k (get r 0 k +. c))) a;
    Hashtbl.replace h w (r, max t t');
  ) wk_updates;
  let wk_updates' = ref [] in
  Hashtbl.iter (fun w (r,t) ->
    let a = Array.make (MD.nnz r) (0,0.) in
    let j = ref 0 in
    MD.iteri (fun _ k c ->
      if c <> 0. then (a.(!j) <- (k,c); j := !j + 1)
    ) r;
    wk_updates' := !wk_updates' @ [(w,a,t)]
  ) h;
  !wk_updates'

let push _context params =
  Actor_logger.info "push @ %s" !_context.master_addr;
  show_stats ();
  (* a workaround for t__k at the moment *)
  let t_k' = P2P.get (-1) |> fst in
  t__k := MD.copy t_k';
  (* if there are words to be merged into global model, rebuild local one *)
  let shall_rebuild = ref false in
  Array.iter (fun b -> if b = false then shall_rebuild := true) !b__m;
  if !shall_rebuild then rebuild_local_model ();
  (* update local model and set bitmap of words *)
  let h = Array.make !n_v false in
  List.iteri (fun i (w,a) ->
    if !b__m.(w) = true then (
      Array.iter (fun (k,c) -> MD.set !t_wk w k c) a
    )
    else (
      Array.iter (fun (k,c) ->
        MD.(set !t_wk w k (get !t_wk w k +. c));
        MD.(set !t__k 0 k (get !t__k 0 k +. c))
      ) a;
      !b__m.(w) <- true
    );
    h.(w) <- true;
  ) params;
  (* iterate all local docs, time-consuming *)
  for j = 0 to !n_d - 1 do sampling j h done;
  (* calculate model updates *)
  let updates = ref [] in
  List.iter (fun (w,a) ->
    let h = Array.make !n_k false in
    let a' = ref [||] in
    Array.iter (fun (k,c) ->
      h.(k) <- true;
      let c' = MD.get !t_wk w k in
      if c' <> c then a' := Array.append !a' [|(k,c'-.c)|]
    ) a;
    let r = MD.row !t_wk w in
    MD.iteri (fun _ k c' ->
      if h.(k) = false && c' <> 0. then a' := Array.append !a' [|(k,c')|]
    ) r;
    updates := !updates @ [(w,!a')]
  ) params;
  (* calculate t__k updates *)
  let a = ref [||] in
  MD.iteri (fun _ k c ->
    if c <> 0. then a := Array.append !a [|(k,c)|]
  ) MD.(!t__k - t_k');
  updates := !updates @ [(-1,!a)];
  !updates

let barrier _context = Actor_barrier.p2p_bsp _context

let stop _context = !_context.step > 5_00

let start jid =
  (* register schedule, push, pull functions *)
  P2P.register_barrier barrier;
  P2P.register_schedule schedule;
  P2P.register_push push;
  P2P.register_pull pull;
  P2P.register_stop stop;
  (* start running the ps *)
  Actor_logger.info "P2P: lda algorithm starts running ...";
  P2P.start jid Actor_config.manager_addr
