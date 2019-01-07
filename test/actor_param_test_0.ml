
open Actor_param_types

module M = Actor_param.Make (Actor_net_zmq) (Actor_sys_unix)


let main args =
  Owl_log.(set_level DEBUG);
  Random.self_init ();

  let myself = args.(1) in
  let server = args.(2) in
  let client = Array.sub args 3 (Array.length args - 3) in
  let waiting = Hashtbl.create 128 in

  let book = Hashtbl.create 128 in
  Hashtbl.add book server "tcp://127.0.0.1:5555";

  if myself <> server then (
    let port = string_of_int (6000 + Random.int 1000) in
    let addr = "tcp://127.0.0.1:" ^ port in
    Hashtbl.add book myself addr
  )
  else (
    Array.iter (fun key ->
      Hashtbl.add waiting key "waiting"
    ) client
  );

  let context = {
    myself;
    server;
    client;
    book;
    waiting;
  }
  in

  Lwt_main.run (M.init context)


let _ =
  main Sys.argv
