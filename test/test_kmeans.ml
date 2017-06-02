(** [ Naive K-means implementation ]
*)

module Ctx = Actor_mapre

let print_points = List.iter (fun x -> Printf.printf "(%.2f,%.2f)\n" (fst x) (snd x))
let distance x y = (((fst x) -. (fst y)) ** 2.) +. (((snd x) -. (snd y)) ** 2.)
let add_2pts x y = ( ((fst x)+.(fst y)), ((snd x)+.(snd y)) )

let format_filter_data fname = fname
  |> Ctx.flatmap Str.(split (regexp "[\r\n]"))
  |> Ctx.map Str.(split (regexp "[,]"))
  |> Ctx.map (fun x -> (float_of_string (List.nth x 0),float_of_string (List.nth x 1)))
  |> Ctx.filter (fun _ -> Random.float 1. < 0.1)

let kmeans x =
  let centers = ref [(1.,1.); (2.,2.); (3.,3.); (4.,4.)] in
  for i = 0 to 50 do
    let bc = Ctx.broadcast !centers in
    let y = Ctx.map (fun x ->
      let k = ref (-1, max_float) in
      Ctx.get_value bc |> List.iteri (fun i y ->
        let d = distance x y in
        if d < (snd !k) then k := (i,d) );
      (fst !k, (x, 1.)) ) x in
    let y = Ctx.reduce_by_key (fun x y -> (add_2pts (fst x) (fst y), (snd x)+.(snd y)) ) y in
    centers := Ctx.collect y
      |> List.flatten
      |> List.map (fun (k,v) -> let p, c = v in ((fst p)/.c,(snd p)/.c) )
  done;
  !centers

let _ =
  Ctx.init Sys.argv.(1) "tcp://localhost:5555";
  Ctx.load "unix://data/kmeans.data" |> format_filter_data |> kmeans |> print_points;
  Ctx.terminate ()
