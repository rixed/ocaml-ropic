(* Tests *)
open Batteries
open Ropic
module L = Log.Make (Log.ToStdout)
module E = Event.Make (L.Debug)

(* Check UdpClient *)

module Udp_Checks =
struct
    (* We simulate an echo server *)
    module Types =
    struct
        type to_write = string
        type to_read = string
        let serialize = Pdu.Marshaler.serialize
        let unserialize = Pdu.Marshaler.unserialize
    end
    module Clt = Event.UdpClient (Types) (E)
    module Srv = Event.UdpServer (Types) (E)
    let checks () =
        let service = Address.make "localhost" 31142 in
        let stop_listening = ref ignore in
        let server _addr respond = function
            | EndOfFile ->
                OUnit2.assert_failure "Server received EOF"
            | Value s ->
                E.L.debug "Echoing string '%s'" s ;
                respond (Write s) ;
                !stop_listening () in
        stop_listening := Srv.serve service server ;

        let test_str = "glop glop" in
        let send = Clt.client service (fun w res ->
            match res with
            | EndOfFile ->
                OUnit2.assert_failure "Client received EOF"
            | Value l ->
                OUnit2.assert_equal ~msg:"String equals" l test_str ;
                E.L.debug "Received echo of '%s' = '%s'" test_str l ;
                w Close) in
        send (Write test_str) ;
end

(* Check TcpClient *)

module Tcp_Checks =
struct
    (* We simulate a 'string-length' service: client sends string and read their length *)
    module CltTypes =
    struct
        type to_write = string
        type to_read = int
        let serialize = Pdu.Marshaler.serialize
        let unserialize = Pdu.Marshaler.unserialize
    end
    module SrvTypes =
    struct
        type to_write = int
        type to_read = string
        let serialize = Pdu.Marshaler.serialize
        let unserialize = Pdu.Marshaler.unserialize
    end
    module Clt = Event.TcpClient (CltTypes) (E)
    module Srv = Event.TcpServer (SrvTypes) (E)
    let checks () =
        let idx = ref 0 in
        let tests = [| "hello" ; "glop" ; "" |] in
        let service = Address.make "localhost" 31142 in
        let stop_listening = ref ignore in
        let server _addr w = function
            | EndOfFile ->
                w Close ;
                !stop_listening ()
            | Value s ->
                E.L.debug "Serving string.length for str='%s'" s ;
                Write (String.length s) |> w in
        stop_listening := Srv.serve service server ;
        let send = Clt.client service (fun _w res ->
            match res with
            | EndOfFile -> () (* we already closed at the beginning *)
            | Value l ->
                OUnit2.assert_equal ~msg:"String length" l (String.length tests.(!idx)) ;
                incr idx) in
        Array.iter (fun s -> send (Write s)) tests ;
        send Close
end

(* Check RPCs *)

module RPC_Checks (CltMaker : Event.CLIENT_MAKER)
                  (SrvMaker : Event.SERVER_MAKER) =
struct
    module RPC_Types =
    struct
        type arg = int * int
        type ret = string
    end
    module CltTypes = Pdu.MarshalCltTypes (RPC_Types)
    module SrvTypes = Pdu.MarshalSrvTypes (RPC_Types)
    module RPC_Clt = Rpc.Client (E) (CltTypes) (CltMaker)
    module RPC_Srv = Rpc.Server (E) (SrvTypes) (SrvMaker)

    let host = Address.make "localhost" 21743

    let shutdown = ref ignore

    let () =
        let f _addr ~deadline w (a, b) = Ok (String.of_int (a+b)) |> w in
        shutdown := RPC_Srv.serve host f

    let checks () =
        RPC_Clt.call host (0, 1) (fun r ->
            E.L.debug "Test RPC(0,1)" ;
            OUnit2.assert_equal ~msg:"RPC answer is OK, 1" r (Ok "1")) ;
        RPC_Clt.call host (2, 3) (fun r ->
            E.L.debug "Test RPC(2,3)" ;
            OUnit2.assert_equal ~msg:"RPC answer is OK, 5" r (Ok "5") ;
            (* And we can call an RPC from an answer *)
            RPC_Clt.call host (4, 5) (fun r ->
                E.L.debug "Test RPC(4,5)" ;
                OUnit2.assert_equal ~msg:"RPC answer is OK, 9" r (Ok "9") ;
            !shutdown ()))
end

let () =
    let module R = RPC_Checks (Event.TcpClient) (Event.TcpServer) in
    R.checks () ;
    Udp_Checks.checks () ;
    Tcp_Checks.checks () ;
    E.loop ()

