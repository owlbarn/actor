(** [ Test parameter server ]  *)

module PS = Paramclient

let test () =
  let _ = PS.set (1,1) "abcd" 1 in
  let _ = PS.get (1,1) 2 in
  Logger.debug "%s" "test done"

let _ = test ()
