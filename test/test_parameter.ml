(** [ Test parameter server ]  *)

module PS = Parameter

let test_store () =
  let _ = PS.set (1,1) "abcd" in
  let _ = PS.get (1,1) in
  Logger.debug "%s" "test done"

let schedule workers =
  let tasks = List.map (fun x ->
    let k, v = Random.int 100, Random.int 1000 in (x, [(k,v)])
  ) workers in tasks

let push id vars =
  let updates = List.map (fun (k,v) ->
    Logger.info "working on %i" v;
    (k,v) ) vars in
  updates

let test_context () =
  PS.register_schedule schedule;
  PS.register_push push;
  PS.init Sys.argv.(1) Config.manager_addr;
  Logger.info "do some work at master node"

let _ = test_context ()
