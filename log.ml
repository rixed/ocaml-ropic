open Batteries

let no_color = "\027[0m"
let color bold c = "\027["^ (if bold then "1;" else "0;") ^ (30 + c |> string_of_int) ^"m"
let colored bold c str = color bold c ^str^ no_color

let start_of_time =
    Unix.gettimeofday () *. 1_000_000. |> Int64.of_float

(* Print time as number of microseconds *)
let date fmt d =
    let d = Int64.of_float (d *. 1_000_000.) in
    Printf.fprintf fmt "%8Ld" Int64.(d - start_of_time)

let file fmt fd =
    let fd = Obj.magic (fd : Unix.file_descr) in
    Printf.fprintf fmt "%d" fd

let log fmt =
    Printf.eprintf "%a: " date (Unix.gettimeofday ()) ;
    Printf.eprintf (fmt^^"\n%!")

let nolog fmt = Printf.ifprintf stderr fmt

module Debug =
struct
    let debug = log
    let info = log
    let error = log
end

module Info =
struct
    let debug = nolog
    let info = log
    let error = log
end

module Error =
struct
    let debug = nolog
    let info = nolog
    let error = log
end

