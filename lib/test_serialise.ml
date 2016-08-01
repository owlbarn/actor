(** [ test_serialise ]
  test the serialisaton function
*)

let test () = print_endline "I am a test fun."

let server () =
  let context = ZMQ.Context.create () in
  let responder = ZMQ.Socket.create context ZMQ.Socket.rep in
  ZMQ.Socket.bind responder "tcp://*:5555";
  while true do
    let request = ZMQ.Socket.recv responder in
    Printf.printf "Received request: [%s]\n%!" request;
    let s = Marshal.to_string test [ Marshal.Closures ] in
    ZMQ.Socket.send responder s;
  done;
  ZMQ.Socket.close responder;
  ZMQ.Context.terminate context

let client () =
  let context = ZMQ.Context.create () in
  print_endline "Connecting to hello world server...";
  let requester = ZMQ.Socket.create context ZMQ.Socket.req in
  ZMQ.Socket.connect requester "tcp://localhost:5555";
  for i = 1 to 5 do
    Printf.printf "Sending request %d...\n" i;
    ZMQ.Socket.send requester "Hello";
    let s = ZMQ.Socket.recv requester in
    Printf.printf "call the serialized function %i ... \n" i;
    let f : unit -> unit = Marshal.from_string s 0 in
    f ()
  done;
  ZMQ.Socket.close requester;
  ZMQ.Context.terminate context

let _ =
  match Sys.argv.(1) with
    | "0" -> server ()
    | "1" -> client ()
    | _   -> print_endline "unknown"
