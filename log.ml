open Batteries

type ('a, 'b) printer = ('a, 'b BatInnerIO.output, unit, unit, unit, unit) Batteries.format6 -> 'a

module type S =
sig
  (* log level >= 1 *)
  val panic : ('a, 'b) printer
  (* log level >= 2 *)
  val error : ('a, 'b) printer
  (* log level >= 3 *)
  val warn : ('a, 'b) printer
  (* log level >= 4 *)
  val info : ('a, 'b) printer
  (* log level >= 5 *)
  val debug : ('a, 'b) printer
end

module type BASE =
sig
  val print : ('a, 'b) printer
end

let dont_print fmt = Printf.ifprintf stdout fmt

(* printer for file descriptors *)
let file fmt fd =
  let fd = Obj.magic (fd : Unix.file_descr) in
  Printf.fprintf fmt "%d" fd

module ToStdout =
struct
  let date fmt d =
    let open Unix in
    let tm = localtime d in
    Printf.fprintf fmt "%04d-%02d-%02d %02dh%02dm%02d.%03ds"
      (tm.tm_year + 1900)
      (tm.tm_mon + 1)
      tm.tm_mday tm.tm_hour tm.tm_min tm.tm_sec
      (int_of_float ((mod_float d 1.) *. 1000.))
    
  let print fmt =
    Printf.printf "%a: " date (Unix.gettimeofday ()) ;
    Printf.printf (fmt ^^ "\n%!")
end

module Debug (Base : BASE) : S =
struct
  let panic fmt = 
    Base.print fmt ;
    exit 1

  let error = Base.print
  let warn =  Base.print
  let info =  Base.print
  let debug = Base.print
end

module Info (Base : BASE) : S =
struct
  let panic fmt = 
    Base.print fmt ;
    exit 1

  let error = Base.print
  let warn =  Base.print
  let info =  Base.print
  let debug = dont_print
end

module Warn (Base : BASE) : S =
struct
  let panic fmt = 
    Base.print fmt ;
    exit 1

  let error = Base.print
  let warn =  Base.print
  let info =  dont_print
  let debug = dont_print
end

module Error (Base : BASE) : S =
struct
  let panic fmt = 
    Base.print fmt ;
    exit 1

  let error = Base.print
  let warn =  dont_print
  let info =  dont_print
  let debug = dont_print
end

