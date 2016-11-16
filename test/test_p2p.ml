(** [ Test parameter server ]  *)

module P2P = Peer

let schedule id =
  Logger.debug "%s: scheduling ..." id;
  Unix.sleep 1;
  []

let push id params =
  Logger.debug "%s: working ..." id;
  Unix.sleep 1;
  let k = Random.int 100 in
  [ (k,0.) ]

let test_context () =
  P2P.register_schedule schedule;
  P2P.register_push push;
  P2P.start Sys.argv.(1) Config.manager_addr

let _ = test_context ()
