(* All signatures for the whole library *)

module Address =
struct
    type t = { name : string ;
               port : int }
    let make name port = { name ; port }
    let to_string t = t.name ^":"^ string_of_int t.port
    let of_sockaddr = function
        | Unix.ADDR_UNIX _fname -> failwith "TODO"
        | Unix.ADDR_INET (ip, port) ->
            { name = Unix.string_of_inet_addr ip ; port }
end

(* Retriable errors will be retried *)
type 'a res = Ok of 'a
            | Timeout
            | Err of string

module RPC =
struct
    module type TYPES =
    sig
        type arg
        type ret
    end

    module type S =
    sig
        module Types : TYPES

        (* We favor event driven programming here *)
        val call : ?timeout:float -> Address.t -> Types.arg -> (Types.ret res -> unit) -> unit
        (* TODO: a call_multiple that allows several answers to be received. Useful to
         * implement pubsub *)

        (* TODO: add the timeout callback here so that we can call it only when the fd is empty *)
        (* First argument is the adress we want to serve from,
         * Second is the query service funtion, which takes the remote address
         * the query is coming from, the continuation, and the argument in last
         * position for easier pattern matching. *)
        val serve : Address.t -> (Address.t -> (Types.ret -> unit) -> Types.arg -> unit) -> unit
    end

    module type Maker = functor(Types : TYPES) -> (S with module Types = Types)
end

module type BaseIOType =
sig
    type t_read
    type t_write
end

module type IOType =
sig
    include BaseIOType
    type read_result = Value of t_read | Timeout of float | EndOfFile
    type write_cmd = Write of t_write | Close
end

module MakeIOType (B : BaseIOType) : IOType with type t_read = B.t_read and type t_write = B.t_write =
struct
    include B
    type read_result = Value of t_read | Timeout of float | EndOfFile
    type write_cmd = Write of t_write | Close
end

module MakeIOTypeRev (B : BaseIOType) : IOType with type t_read = B.t_write and type t_write = B.t_read =
struct
    module BaseIOTypeRev =
    struct
        type t_read = B.t_write
        type t_write = B.t_read
    end
    include MakeIOType (BaseIOTypeRev)
end

module type PDU =
sig
    module BaseIOType : BaseIOType (* the types of value transported in this PDU *)

    val to_string : BaseIOType.t_write -> string

    (* tells if there are enough bytes for reading a value (total size returned) *)
    val has_value : string -> int -> int -> int option

    (* once we know a value is available, read it *)
    val of_string : string -> int -> int -> BaseIOType.t_read option
end
