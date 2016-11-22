(** [ Test LDA on P2P parallel ]  *)

open Owl
open Types

(* load stopwords, load data, build dict, tokenisation *)
let s = Owl_topic_utils.load_stopwords "/Users/liang/code/owl/lib/topic/stopwords.txt"
let x = Owl_topic_utils.load_data ~stopwords:s "/Users/liang/code/experimental-lda/data/nips.train"
let v = Owl_topic_utils.build_vocabulary x
let d = Owl_topic_utils.tokenisation v x
let t = 100

let _ =
  Logger.info "#doc:%i #top:%i #voc:%i" (Array.length d) t (Hashtbl.length v);
  Peer_lda.init t v d;
  Peer_lda.start Sys.argv.(1)
