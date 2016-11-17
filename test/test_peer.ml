(** [ Test P2P parallel ]  *)

module P2P = Peer

let schedule id =
  (* Logger.debug "%s: scheduling ..." id; *)
  Unix.sleep 5; []

let test_context () =
  P2P.register_schedule schedule;
  P2P.start Sys.argv.(1) Config.manager_addr

let _ = test_context ()
