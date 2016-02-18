(* Tests *)
open Batteries
open Ropic
module L = Log.Debug (Log.ToStdout)
module E = Event.Make (L)

(* Check UdpClient *)

module Udp_Checks =
struct
    (* We simulate an echo server *)
    module BaseIOType =
    struct
        type t_write = string
        type t_read = string
    end
    module CltT = MakeIOType (BaseIOType)
    module Clt_Pdu = Pdu.Marshaller(CltT) (L)
    module Clt = Event.UdpClient (CltT) (Clt_Pdu) (E)
    module SrvT = MakeIOTypeRev (BaseIOType)
    module Srv_Pdu = Pdu.Marshaller(SrvT) (L)
    module Srv = Event.UdpServer (SrvT) (Srv_Pdu) (E)
    let checks () =
        let service_port = "31142" in
        let stop_listening = ref ignore in
        let server respond = function
            | SrvT.Timeout _ ->
                OUnit2.assert_failure "Server received timeout"
            | SrvT.EndOfFile ->
                OUnit2.assert_failure "Server received EOF"
            | SrvT.Value s ->
                L.debug "Echoing string '%s'" s ;
                respond (SrvT.Write s) ;
                !stop_listening () in
        stop_listening := Srv.serve service_port server ;

        let test_str = "glop glop" in
        let send = Clt.client "localhost" service_port (fun w res ->
            match res with
            | CltT.Timeout _ ->
                OUnit2.assert_failure "Client received timeout"
            | CltT.EndOfFile ->
                OUnit2.assert_failure "Client received EOF"
            | CltT.Value l ->
                OUnit2.assert_equal ~msg:"String equals" l test_str ;
                L.debug "Received echo of '%s' = '%s'" test_str l ;
                w CltT.Close) in
        send (CltT.Write test_str) ;
end

(* Check TcpClient *)

module Tcp_Checks =
struct
    (* We simulate a 'string-length' service: client sends string and read their length *)
    module BaseIOType =
    struct
        type t_write = string
        type t_read = int
    end
    module CltT = MakeIOType (BaseIOType)
    module Clt_Pdu = Pdu.Marshaller(CltT) (L)
    module Clt = Event.TcpClient (CltT) (Clt_Pdu) (E)
    module SrvT = MakeIOTypeRev (BaseIOType)
    module Srv_Pdu = Pdu.Marshaller(SrvT) (L)
    module Srv = Event.TcpServer (SrvT) (Srv_Pdu) (struct type t = unit end) (E)
    let checks () =
        let idx = ref 0 in
        let tests = [| "hello" ; "glop" ; "" |] in
        let service_port = "31142" in
        let stop_listening = ref ignore in
        let server () w = function
            | SrvT.Timeout _
            | SrvT.EndOfFile ->
                w SrvT.Close ;
                !stop_listening ()
            | SrvT.Value s ->
                L.debug "Serving string.length for str='%s'" s ;
                SrvT.Write (String.length s) |> w in
        stop_listening := Srv.serve service_port ignore server ;
        let send = Clt.client "localhost" service_port (fun _w res ->
            match res with
            | CltT.Timeout _
            | CltT.EndOfFile -> () (* we already closed at the beginning *)
            | CltT.Value l ->
                OUnit2.assert_equal ~msg:"String length" l (String.length tests.(!idx)) ;
                incr idx) in
        Array.iter (fun s -> send (CltT.Write s)) tests ;
        send CltT.Close
end

(* Check RPCs *)

module RPC_Checks (RPC_Maker : RPC.Maker) =
struct
    module RPC_Types =
    struct
        type arg = int * int
        type ret = string
    end
    module RPC = RPC_Maker (RPC_Types)

    let host = Address.make "localhost" 21743

    let () =
        let f _addr w (a, b) = String.of_int (a+b) |> w in
        RPC.serve host f

    let checks () =
        RPC.call host (0, 1) (fun r ->
            L.debug "Test RPC(0,1)" ;
            OUnit2.assert_equal ~msg:"RPC answer is OK, 1" r (Ok "1")) ;
        RPC.call host (2, 3) (fun r ->
            L.debug "Test RPC(2,3)" ;
            OUnit2.assert_equal ~msg:"RPC answer is OK, 5" r (Ok "5") ;
            (* And we can call an RPC from an answer *)
            RPC.call host (4, 5) (fun r ->
                L.debug "Test RPC(4,5)" ;
                OUnit2.assert_equal ~msg:"RPC answer is OK, 9" r (Ok "9")))
end

let () =
    let module TcpConfig =
        struct
            include Rpc.DefaultTcpConfig
            let cnx_timeout = 0.2
            let max_accepted = Some 1
        end in
    let module R = RPC_Checks (Rpc.Tcp (TcpConfig) (E)) in R.checks () ;
    Udp_Checks.checks () ;
    Tcp_Checks.checks () ;
    E.loop ()

