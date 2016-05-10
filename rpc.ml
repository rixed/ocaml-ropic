open Batteries
open Ropic

(* RPCs are mere communication channels with a way to associate each
 * queries with an answer using a sequence identifier that we add to
 * the type of the argument and return value, and a way to return errors.
 * Therefore, the provided (un)serializer must take into account this
 * additional this additional integer and error variant. *)

(* for clarity, have a single id for any kind of RPC *)
let next_id =
    let n = ref 0 in
    fun () -> incr n ; !n

module type TYPES =
sig
    type arg
    type ret
end

module type TYPES_CLT =
sig
    include TYPES
    type to_write = int (* RPC id *) * float (* deadline *) * arg
    type to_read = int * ret res
    val serialize : to_write -> string
    val unserialize : string -> int -> int -> (int * to_read) option
end
module type TYPES_SRV =
sig
    include TYPES
    type to_write = int * ret res
    type to_read = int * float * arg
    val serialize : to_write  -> string
    val unserialize : string -> int -> int -> (int * to_read) option
end

module S =
struct
    module type CLT =
    sig
        module Types : TYPES_CLT
        val call : ?timeout:float -> ?deadline:float -> Address.t -> Types.arg -> (Types.ret res -> unit) -> unit
        (* TODO: a call_multiple that allows several answers to be received. Useful to implement pubsub *)
        (* TODO: add the timeout callback here so that we can call it only when the fd is empty *)
    end

    module type SRV =
    sig
        module Types : TYPES_SRV

        (* First argument is the address we want to serve from,
         * Second is the query service function, which takes the remote address
         * the query is coming from, the continuation and the argument
         * in last position for easier pattern matching.
         * Returns a shutdown function. *)
        val serve : Address.t ->
                    (Address.t ->
                     deadline:float ->
                     (Types.ret res -> unit) ->
                     Types.arg ->
                     unit) ->
                    (unit -> unit)
    end
end

module Server (E : Event.S)
              (Types : TYPES_SRV)
              (SrvMaker : Event.SERVER_MAKER) :
    S.SRV with module Types = Types =
struct
    module Types = Types
    module Server = SrvMaker (Types) (E)

    let serve h f =
        Server.serve h (fun addr write -> function
            | Value (id, deadline, v) ->
                let now = Unix.gettimeofday () in
                if deadline < now then
                  E.L.warn "message id %d is dead on arrival" id
                else
                  f addr ~deadline (fun res -> write (Write (id, res))) v
            | EndOfFile ->
                write Close)
end

module Client (E : Event.S)
              (Types : TYPES_CLT)
              (CltMaker : Event.CLIENT_MAKER) :
    S.CLT with module Types = Types =
struct
    module Types = Types
    module Client = CltMaker (Types) (E)

    (* Notice that:
     * - the TCP cnx is initialized when first used and then saved for later,
     *   so that it's easier for the client (no need to keep out state along) and
     *   also it doesn't have to explicitly connect to a new place and handle the connection
     *   termination itself. This cost a hashtable lookup, though.
     * - we need to associate an id with each query and store every continuations in a hash to send the proper
     *   answer to the proper continuation, since you may call the server several times before an answer is
     *   received, and the server is not constrained to answer in sequence (since it may itself depend on a
     *   backend). Since the hash of cnx is global and the id is global as well, we can imagine a query
     *   being answered by another server, which is cool or frightening.
     * - as a result, if we store several servers on this program they can share the same cnxs if they
     *   speak to the same dest
     *)
    let cnxs = Hashtbl.create 31
    let continuations = Hashtbl.create 72

    (* timeout continuations *)
    let try_timeout id =
        match Hashtbl.find_option continuations id with
        | Some k ->
            E.L.warn "Timeouting message id %d" id ;
            Hashtbl.remove continuations id ;
            k (Err "timeout")
        | None -> ()

    (* timeout = 0 means we expect no answer other than an error if we cannot send.
     * FIXME: a Maker wrapper for when the return type is unit, that will force this timeout to 0. *)
    let call ?(timeout=0.5) ?deadline h v k =
        (* We propagate deadlines instead of timeouts because we assume NTP synchronized machines.
         * NTP over internet is accurate to a few milliseconds, which is good enough for timeouting
         * RPCs. Of course there are issues during leap seconds, especially if your timeout is less
         * than a few seconds. Leap seconds are easy to detect though, and we could replace
         * gettimeofday with a function that would smooth leap seconds away. TODO. *)
        let now = Unix.gettimeofday () in
        let deadline_to = now +. timeout in
        let deadline = match deadline with
            | None -> deadline_to
            | Some d -> min d deadline_to in
        let writer =
            match Hashtbl.find_option cnxs h with
            | Some w -> w
            | None ->
                (* connect to the server *)
                E.L.debug "Need a new connection to %s" (Address.to_string h) ;
                let w = Client.client h (fun write input ->
                    match input with
                    | Value (id, v) ->
                        if id = 0 then (
                          E.L.error "Received an answer for a query that was not expecting one"
                        ) else
                        (* Note: we can't modify continuations in place because we'd
                         * have to call write before returning to modify_opt, and write
                         * can itself update the hash. *)
                        (match Hashtbl.find_option continuations id with
                        | None ->
                            E.L.warn "No continuation for message id %d (already timeouted?)" id
                        | Some k ->
                            E.L.debug "Continuing message id %d" id ;
                            k v ;
                            Hashtbl.remove continuations id)
                    | EndOfFile ->
                        (* since we don't know which messages were sent via this cnx, rely on timeout to notify continuations *)
                        E.L.info "Closing cnx to %s" (Address.to_string h) ;
                        write Close ;
                        Hashtbl.remove cnxs h) in
                Hashtbl.add cnxs h w ;
                w in
        let id = if timeout > 0. then (
          let id = next_id () in
          E.L.debug "Saving continuation for message id %d with timeout in %f" id timeout ;
          E.at deadline (fun () -> try_timeout id) ;
          Hashtbl.add continuations id k ;
          id
        ) else 0 in
        writer (Write (id, deadline, v))
end

