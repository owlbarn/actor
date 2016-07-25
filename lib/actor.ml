
let test () = print_endline "I am an actor."

let to_string f = Marshal.to_string f [ Marshal.Closures ]

let from_string s =
  let f : unit -> unit = Marshal.from_string s 0 in
  f
