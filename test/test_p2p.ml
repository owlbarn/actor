(** [ Test parameter server ]  *)

module P2P = Peer

let test_context () =
  P2P.start Sys.argv.(1) Config.manager_addr

let _ = test_context ()
