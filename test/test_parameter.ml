(** [ Test parameter server ]  *)

module PS = Parameter
module PC = Paramclient

let test_store () =
  let _ = PC.set (1,1) "abcd" 1 in
  let _ = PC.get (1,1) 2 in
  Logger.debug "%s" "test done"

let schedule workers = workers

let test_context () =
  PS.register_schedule schedule;
  PS.init Sys.argv.(1) Config.manager_addr;
  Logger.info "%s" "do some work ..."

let _ = test_context ()
