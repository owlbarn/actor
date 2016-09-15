(** [ Test parameter server ]  *)

module PS = Parameter

let test () =
  PS.set (1,1) "abcd" 1; Unix.sleep 1

let _ = test ()
