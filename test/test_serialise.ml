(** [ test_serialise ]
  test the serialisaton function
*)

let test () = print_endline "I am a test fun."

let server () =
  let context = Zmq.Context.create () in
  let responder = Zmq.Socket.create context Zmq.Socket.rep in
  Zmq.Socket.bind responder "tcp://*:5555";
  while true do
    let request = Zmq.Socket.recv responder in
    Printf.printf "Received request: [%s]\n%!" request;
    let s = Marshal.to_string test [ Marshal.Closures ] in
    Zmq.Socket.send responder s;
  done;
  Zmq.Socket.close responder;
  Zmq.Context.terminate context

let client () =
  let context = Zmq.Context.create () in
  print_endline "Connecting to hello world server...";
  let requester = Zmq.Socket.create context Zmq.Socket.req in
  Zmq.Socket.connect requester "tcp://localhost:5555";
  for i = 1 to 5 do
    Printf.printf "Sending request %d...\n" i;
    Zmq.Socket.send requester "Hello";
    let s = Zmq.Socket.recv requester in
    Printf.printf "call the serialized function %i ... \n" i;
    let f : unit -> unit = Marshal.from_string s 0 in
    f ()
  done;
  Zmq.Socket.close requester;
  Zmq.Context.terminate context

let _ =
  match Sys.argv.(1) with
    | "0" -> server ()
    | "1" -> client ()
    | _   -> print_endline "unknown"
