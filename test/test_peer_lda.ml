(** [ Test LDA on P2P parallel ]  *)

open Owl
open Actor_types

(* load stopwords, load data, build dict, tokenisation *)
let s = Dataset.load_stopwords ()
let x = Dataset.load_nips_train_data s
let v, _ = Owl_nlp_utils.build_vocabulary x
(* only choose 30% data to train
let x = Stats.choose x (Array.length x / 3) *)
let d = Owl_nlp_utils.tokenise_all v x
let t = 100

let _ =
  Actor_logger.info "#doc:%i #top:%i #voc:%i" (Array.length d) t (Hashtbl.length v);
  Actor_peer_lda.init t v d;
  Actor_peer_lda.start Sys.argv.(1)
