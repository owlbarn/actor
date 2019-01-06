

module M = Actor_mapre.Make (Actor_net_zmq) (Actor_sys_unix)

open M.Types


let main args =
  Owl_log.(set_level DEBUG);
  let myself = args.(1) in
  let server = args.(2) in
  let client = Array.sub args 3 (Array.length args - 3) in
  let waiting = Hashtbl.create 128 in

  let uri = ref StrMap.empty in
  uri := StrMap.add server "tcp://127.0.0.1:5555" !uri;

  if myself <> server then (
    let addr = "tcp://127.0.0.1:5557" in
    uri := StrMap.add myself addr !uri
  )
  else (
    Array.iter (fun key ->
      Hashtbl.add waiting key "waiting"
    ) client
  );

  let config = {
    myself;
    server;
    client;
    uri = !uri;
    waiting;
  }
  in

  Lwt_main.run (M.init config)


let _ =
  main Sys.argv
