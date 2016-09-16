(** [ Test parameter server ]  *)

module PS = Paramclient

let test () =
  PS.set (1,1) "abcd" 1; Unix.sleep 1

let _ = test ()
