(** [ Test parameter server ]  *)

module PS = Parameter

let test_store () =
  let _ = PS.set (1,1) "abcd" in
  let _ = PS.get (1,1) in
  Logger.debug "%s" "test done"

let schedule workers =
  let l = List.length workers in
  let x = Random.int l in
  let y = (x + 1) mod l in
  [ (List.nth workers x, ["task1"]); (List.nth workers y, ["task2"]) ]

let push id vars =
  Logger.info "do some useful stuff ...";
  ["abc"; "def"]

let test_context () =
  PS.register_schedule schedule;
  PS.register_push push;
  PS.init Sys.argv.(1) Config.manager_addr;
  Logger.info "%s" "do some work ..."

let _ = test_context ()
