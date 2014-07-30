open Batteries
open Ropic

module L = Log.Info

module Marshaller (T : BaseIOType) : PDU with module BaseIOType = T =
struct
    module BaseIOType = T

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

    let has_value str ofs avail_len =
        if avail_len < header_size then None else
        let len, _ = read_header str ofs in
        assert (len >= Marshal.header_size) ;
        let value_len = header_size + len in
        if avail_len < value_len then None else
        Some value_len

    let of_string str ofs tot_len =
        let len, sum = read_header str ofs in
        assert (tot_len = header_size + len) ;
        if sum <> checksum str (ofs + header_size) len then (
            L.error "Cannot decode value, skipping" ;
            None
        ) else (
            Some (Marshal.from_string str (ofs + header_size))
        )

    let to_string v =
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

module AllStrings = struct
    type t_read = string
    type t_write = string
end

module Blobber : PDU with module BaseIOType = AllStrings =
struct
    module BaseIOType = AllStrings

    let has_value _str _ofs len =
        let max_size = 1000 in
        if len > 0 then Some (min len max_size) else None

    let of_string str ofs len =
        Some (String.sub str ofs len)

    let to_string = identity

end
