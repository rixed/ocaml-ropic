open Batteries

type 'a printer = ('a, unit BatInnerIO.output, unit, unit, unit, unit) Batteries.format6 -> 'a

module type S =
sig
  val error : 'a printer
  val warn : 'a printer
  val info : 'a printer
  val debug : 'a printer
end

module type BASE =
sig
  val print : 'a printer
end

let dont_print fmt = Printf.ifprintf stdout fmt

(* printer for file descriptors *)
let file fmt fd =
  let fd = Obj.magic (fd : Unix.file_descr) in
  Printf.fprintf fmt "%d" fd

let print_date fmt d =
  let open Unix in
  let tm = localtime d in
  Printf.fprintf fmt "%04d-%02d-%02d %02dh%02dm%02d.%03ds"
    (tm.tm_year + 1900)
    (tm.tm_mon + 1)
    tm.tm_mday tm.tm_hour tm.tm_min tm.tm_sec
    (int_of_float ((mod_float d 1.) *. 1000.))

module ToFile (Conf : sig val oc : unit BatIO.output end) =
struct
  let print fmt =
    Printf.fprintf Conf.oc "%a: " print_date (Unix.gettimeofday ()) ;
    Printf.fprintf Conf.oc (fmt ^^ "\n%!")
end

module ToStdout : BASE = ToFile (struct let oc = stdout end)

module ToLogfile (Conf : sig val name : string end) : BASE = ToFile (struct
  let oc = File.open_out ~mode:[`create;`append;`text] Conf.name
end)

module Make (Base : BASE) =
struct
  module Debug : S =
  struct
    let error = Base.print
    let warn =  Base.print
    let info =  Base.print
    let debug = Base.print
  end

  module Info : S =
  struct
    let error = Base.print
    let warn =  Base.print
    let info =  Base.print
    let debug = dont_print
  end

  module Warn : S =
  struct
    let error = Base.print
    let warn =  Base.print
    let info =  dont_print
    let debug = dont_print
  end

  module Error : S =
  struct
    let error = Base.print
    let warn =  dont_print
    let info =  dont_print
    let debug = dont_print
  end
end
