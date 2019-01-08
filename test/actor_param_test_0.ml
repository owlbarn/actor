
open Actor_param_types

module M = Actor_param.Make (Actor_net_zmq) (Actor_sys_unix)


let main args =
  Owl_log.(set_level DEBUG);
  Random.self_init ();

  let port = string_of_int (6000 + Random.int 1000) in
  let random_addr = "tcp://127.0.0.1:" ^ port in

  let server_uuid = args.(2) in
  let server_addr = "tcp://127.0.0.1:5555" in
  let my_uuid = args.(1) in
  let my_addr =
    if my_uuid = server_uuid then
      server_addr
    else
      random_addr
  in

  let book = Actor_book.make () in
  let clients = Array.sub args 3 (Array.length args - 3) in
  Array.iter (fun uuid ->
    if uuid = my_uuid then
      Actor_book.add book my_uuid my_addr false (-1)
    else
      Actor_book.add book uuid "" false (-1)
  ) clients;

  let context = {
    my_uuid;
    my_addr;
    server_uuid;
    server_addr;
    book;
    schedule = (fun _ -> [|("n01", "test task")|]);
    push     = (fun x -> Unix.sleep 1; x);
  }
  in

  Lwt_main.run (M.init context)


let _ =
  main Sys.argv
