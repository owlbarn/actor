(** [ Test P2P parallel ]  *)

module P2P = Actor_peer

let schedule id =
  (* Logger.debug "%s: scheduling ..." id; *)
  Unix.sleep 5; []

let test_context () =
  P2P.register_schedule schedule;
  P2P.start Sys.argv.(1) Actor_config.manager_addr

let _ = test_context ()
