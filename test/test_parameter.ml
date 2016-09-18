(** [ Test parameter server ]  *)

module PS = Parameter
module PC = Paramclient

let test_store () =
  let _ = PC.set (1,1) "abcd" 1 in
  let _ = PC.get (1,1) 2 in
  Logger.debug "%s" "test done"

let test_context () =
  PS.init Sys.argv.(1) Config.manager_addr;
  Logger.info "%s" "do some work ...";
  PS.terminate ()

let _ = test_context ()
