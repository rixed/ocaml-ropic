open Batteries
open Ropic

module L = Log.Info

(* RPC through TCP *)

(* for clarity, have a single id for any kind of TCP *)
let next_id =
    let n = ref 0 in
    fun () -> incr n ; !n

(* TODO: Given Tcp and Udp works exactly the same, make this a functor from Event.SERVER and Event.CLIENT (or a functor from a ServerMaker functor, so that we give it the IOTypes and Pdu and E we already have *)
module Tcp (E : Event.S)
           (Types : RPC.TYPES) :
    RPC.S with module Types = Types =
struct
    module Types = Types

    type id = int

    module BaseIOType =
    struct
        type t_read = id * Types.arg
        type t_write = id * Types.ret
    end
    module Srv_IOType = MakeIOType (BaseIOType)
    module Srv_Pdu = Pdu.Marshaller (Srv_IOType) (E.L)
    module TcpServer = Event.TcpServer (Srv_IOType) (Srv_Pdu) (E)

    let serve h f =
        TcpServer.serve (string_of_int h.Address.port) (fun addr write input ->
            match input with
            | Srv_IOType.Value (id, v) ->
                f addr (fun res -> write (Srv_IOType.Write (id, res))) v
            | Srv_IOType.EndOfFile ->
                write Srv_IOType.Close)

    (* Reverse the types and build the client: *)
    module Clt_IOType = MakeIOTypeRev (BaseIOType)
    module Clt_Pdu = Pdu.Marshaller (Clt_IOType) (E.L)
    module TcpClient = Event.TcpClient (Clt_IOType) (Clt_Pdu) (E)

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
            E.L.debug "Timeouting message id %d" id ;
            Hashtbl.remove continuations id ;
            k (Err "timeout")
        | None -> ()

    let call ?(timeout=0.5) h v k =
        let writer =
            match Hashtbl.find_option cnxs h with
            | Some w -> w
            | None ->
                (* connect to the server *)
                E.L.debug "Need a new connection to %s" (Address.to_string h) ;
                let w = TcpClient.client h.Address.name (string_of_int h.Address.port) (fun write input ->
                    match input with
                    | Clt_IOType.Value (id, v) ->
                        (* Note: we can't modify continuations in place because we'd
                         * have to call write before returning to modify_opt, and write
                         * can itself update the hash. *)
                        (match Hashtbl.find_option continuations id with
                        | None ->
                            E.L.error "No continuation for message id %d (already timeouted?)" id
                        | Some k ->
                            E.L.debug "Continuing message id %d" id ;
                            k (Ok v) ;
                            Hashtbl.remove continuations id)
                    | Clt_IOType.EndOfFile ->
                        (* since we don't know which messages were sent via this cnx, rely on timeout to notify continuations *)
                        E.L.info "Closing cnx to %s" (Address.to_string h) ;
                        write Close ;
                        Hashtbl.remove cnxs h) in
                Hashtbl.add cnxs h w ;
                w in
        let id = next_id () in
        E.L.debug "Saving continuation for message id %d" id ;
        E.pause timeout (fun () -> try_timeout id) ;
        Hashtbl.add continuations id k ;
        writer (Write (id, v))
end

module Udp (E : Event.S)
           (Types : RPC.TYPES) :
    RPC.S with module Types = Types =
struct
    module Types = Types

    type id = int

    module BaseIOType =
    struct
        type t_read = id * Types.arg
        type t_write = id * Types.ret
    end
    module Srv_IOType = MakeIOType (BaseIOType)
    module Srv_Pdu = Pdu.Marshaller (Srv_IOType) (E.L)
    module UdpServer = Event.UdpServer (Srv_IOType) (Srv_Pdu) (E)

    let serve h f =
        UdpServer.serve (string_of_int h.Address.port) (fun addr write input ->
            match input with
            | Srv_IOType.Value (id, v) ->
                f addr (fun res -> write (Srv_IOType.Write (id, res))) v
            | Srv_IOType.EndOfFile ->
                write Srv_IOType.Close)

    (* Reverse the types and build the client: *)
    module Clt_IOType = MakeIOTypeRev (BaseIOType)
    module Clt_Pdu = Pdu.Marshaller (Clt_IOType) (E.L)
    module UdpClient = Event.UdpClient (Clt_IOType) (Clt_Pdu) (E)

    (* Compared to TCP, our "cnxs" are writer functions that saves us from
     * name resolution, but of course there are no round-trip connection
     * process involved. *)
    let cnxs = Hashtbl.create 31
    let continuations = Hashtbl.create 72

    (* timeout continuations *)
    let try_timeout id =
        match Hashtbl.find_option continuations id with
        | Some k ->
            E.L.debug "Timeouting message id %d" id ;
            Hashtbl.remove continuations id ;
            k (Err "timeout")
        | None -> ()

    let call ?(timeout=0.5) h v k =
        let writer =
            match Hashtbl.find_option cnxs h with
            | Some w -> w
            | None ->
                (* connect to the server *)
                E.L.debug "Need a new writer to %s" (Address.to_string h) ;
                let w = UdpClient.client h.Address.name (string_of_int h.Address.port) (fun write input ->
                    match input with
                    | Clt_IOType.Value (id, v) ->
                        (* Note: we can't modify continuations in place because we'd
                         * have to call write before returning to modify_opt, and write
                         * can itself update the hash. *)
                        (match Hashtbl.find_option continuations id with
                        | None ->
                            E.L.error "No continuation for message id %d (already timeouted?)" id
                        | Some k ->
                            E.L.debug "Continuing message id %d" id ;
                            k (Ok v) ;
                            Hashtbl.remove continuations id)
                    | Clt_IOType.EndOfFile ->
                        (* since we don't know which messages were sent via this cnx, rely on timeout to notify continuations *)
                        E.L.info "Closing datagram 'cnx' to %s" (Address.to_string h) ;
                        write Close ;
                        Hashtbl.remove cnxs h) in
                Hashtbl.add cnxs h w ;
                w in
        let id = next_id () in
        E.L.debug "Saving continuation for message id %d" id ;
        E.pause timeout (fun () -> try_timeout id) ;
        Hashtbl.add continuations id k ;
        writer (Write (id, v))
end
