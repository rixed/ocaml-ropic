open Batteries
open Ropic

(* This module provides some ready made (un)serializers.
 * And should therefore be renamed into 'Serializers'. *)
module Marshaller =
struct

    let header_size = 4 (* 2 bytes for len then 2 bytes for checksum *)

    let read_header str ofs =
        (* we assume the header can be read *)
        let word str ofs = Char.code str.[ofs] + Char.code str.[ofs+1] lsl 8 in
        word str ofs, word str (ofs+2)

    let checksum str ofs len =
        let stop = ofs + len in
        let rec loop c o =
            if o >= stop then c else
            loop (c + Char.code str.[o]) (o+1) in
        (loop 0 ofs) land 65535

    let unserialize str ofs avail_len =
        if avail_len < header_size then None else
        let len, sum = read_header str ofs in
        assert (len >= Marshal.header_size) ;
        let value_len = header_size + len in
        if avail_len < value_len then None else
        if sum <> checksum str (ofs + header_size) len then (
            (* FIXME: instead of None|Some, have the possibility to return
             * an error *)
            None
        ) else (
            Some (value_len, Marshal.from_string str (ofs + header_size))
        )

    let serialize v =
        let str = Marshal.to_string v [] in
        let word i s o =
            s.[o] <- Char.chr (i land 255) ;
            s.[o+1] <- Char.chr (i lsr 8) in
        let len = String.length str in
        let sum = checksum str 0 len in
        let s = String.create (header_size + len) in
        word len s 0 ;
        word sum s 2 ;
        String.blit str 0 s header_size len ;
        s
end

(* Given the above Marshaller we can build any TYPES_CLT/SRV: *)
(* NOTE: could be the default, with possibility to override (un)serialize *)
module MarshalCltTypes (T : Rpc.TYPES) =
struct
  include T
  type to_write = int * arg
  type to_read = int * ret res
  let serialize (v : to_write) = Marshaller.serialize v
  let unserialize = Marshaller.unserialize
end

module MarshalSrvTypes (T : Rpc.TYPES) =
struct
  include T
  type to_write = int * ret res
  type to_read = int * arg
  let serialize (v : to_write) = Marshaller.serialize v
  let unserialize = Marshaller.unserialize
end
