open Batteries
open Ropic

(* This module provides some ready made (un)serializers.
 * And should therefore be renamed into 'Serializers'. *)
module Marshaler =
struct

    (* 4 bytes for len then 1 bytes for checksum.
     * All that this checksum achieve is to make "sure" we are not unserializing
     * random data.
     * TODO: an id for the RPC that would be given by the server, like a hash
     * of the service name, plus a version number, to make sure we pair appropriate
     * clients and servers? *)
    let header_size = 4 + 1

    let read_header str ofs =
        (* we assume the header can be read *)
        let byte s o = Char.code s.[o] in
        let word s o = byte s (o+0) +
                       byte s (o+1) lsl 8 +
                       byte s (o+2) lsl 16 +
                       byte s (o+3) lsl 24 in
        word str ofs, byte str (ofs+4)

    let checksum str ofs len =
        let stop = ofs + len in
        let rec loop c o =
            if o >= stop then c else
            loop (c + Char.code str.[o]) (o+1) in
        (loop 0 ofs) land 255

    let unserialize str ofs avail_len =
        if avail_len < header_size then None else
        let len, sum = read_header str ofs in
        let value_len = header_size + len in
        if avail_len < value_len then None else
        if sum <> checksum str (ofs + header_size) len then (
            (* FIXME: instead of None|Some, have the possibility to return
             * an error *)
            None
        ) else (
            match Snappy.uncompress_sub str (ofs + header_size) len with
            | exception Snappy.Error _err -> None
            | str ->
              assert (String.length str >= Marshal.header_size) ;
              Some (value_len, Marshal.from_string str 0)
            )

    let serialize v =
        let str = Marshal.to_string v [] in
        let str = Snappy.compress str in
        let byte s o i = s.[o] <- Char.chr (i land 255) in
        let word s o i =
            byte s (o+0) i ;
            byte s (o+1) (i lsr 8) ;
            byte s (o+2) (i lsr 16) ;
            byte s (o+3) (i lsr 24) in
        let len = String.length str in
        let sum = checksum str 0 len in
        let s = String.create (header_size + len) in
        word s 0 len ;
        word s 4 sum ;
        String.blit str 0 s header_size len ;
        s
end

(* Given the above Marshaler we can build any TYPES_CLT/SRV: *)
(* NOTE: could be the default, with possibility to override (un)serialize *)
module MarshalCltTypes (T : Rpc.TYPES) =
struct
  include T
  type to_write = int (* request id *) * float (* deadline *) * int (* criticality *) * arg
  type to_read = int * ret res
  let serialize (v : to_write) = Marshaler.serialize v
  let unserialize = Marshaler.unserialize
end

module MarshalSrvTypes (T : Rpc.TYPES) =
struct
  include T
  type to_write = int * ret res
  type to_read = int * float * int * arg
  let serialize (v : to_write) = Marshaler.serialize v
  let unserialize = Marshaler.unserialize
end
