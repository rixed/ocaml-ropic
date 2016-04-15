(* All signatures for the whole library *)
open Batteries

let print_sockaddr oc addr =
    let open Unix in
    match addr with
    | ADDR_UNIX file -> Printf.fprintf oc "UNIX:%s" file
    | ADDR_INET (addr, port) ->
      Printf.fprintf oc "%s:%d" (string_of_inet_addr addr) port

let print_addrinfo oc ai =
    let open Unix in
    let string_of_family = function
        | PF_UNIX  -> "Unix"
        | PF_INET  -> "IPv4"
        | PF_INET6 -> "IPv6" in
    let string_of_socktype = function
        | SOCK_STREAM -> "stream"
        | SOCK_DGRAM  -> "datagram"
        | SOCK_RAW    -> "raw"
        | SOCK_SEQPACKET -> "seqpacket" in
    let string_of_proto p =
        try (getprotobynumber p).p_name
        with Not_found -> string_of_int p in
    Printf.fprintf oc "%s:%s:%s %a (%s)"
        (string_of_family ai.ai_family)
        (string_of_socktype ai.ai_socktype)
        (string_of_proto ai.ai_protocol)
        print_sockaddr ai.ai_addr
        ai.ai_canonname

(* Retriable errors will be retried *)
type 'a res = Ok of 'a
            | Err of string

type 'a write_cmd = Write of 'a | Close
type 'a read_result = Value of 'a | EndOfFile

module Address =
struct
    type t = { name : string ;
               port : int }
    let make name port = { name ; port }
    exception BadAddress of string
    let of_string s =
      try Scanf.sscanf s "%s@:%d%!" make
      with _ -> (
        try Scanf.sscanf s "[%s@]:%d%!" make
        with _ -> raise (BadAddress s)
      )
    let to_string t = t.name ^":"^ string_of_int t.port
    let print oc t = String.print oc (to_string t)
    let of_sockaddr = function
        | Unix.ADDR_UNIX _fname -> failwith "TODO"
        | Unix.ADDR_INET (ip, port) ->
            { name = Unix.string_of_inet_addr ip ; port }
end

