(* Tests *)
open Batteries
open Ropic
module L = Log.Debug

(* Check TcpClient *)

module Tcp_Checks =
struct
    module BaseIOType =
    struct
        type t_write = string
        type t_read = int
    end
    (* We simulate a 'string-length' service: client sends string and read their length *)
    module CltT = MakeIOType (BaseIOType)
    module Clt_Pdu = Pdu.Marshaller(CltT)
    module Clt = Event.TcpClient (CltT) (Clt_Pdu)
    module SrvT = MakeIOTypeRev (BaseIOType)
    module Srv_Pdu = Pdu.Marshaller(SrvT)
    module Srv = Event.TcpServer (SrvT) (Srv_Pdu) (struct type t = unit end)
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
                OUnit2.assert_equal l (String.length tests.(!idx)) ;
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
            OUnit2.assert_equal r (Ok "1")) ;
        RPC.call host (2, 3) (fun r ->
            L.debug "Test RPC(2,3)" ;
            OUnit2.assert_equal r (Ok "5") ;
            (* And we can call an RPC from an answer *)
            RPC.call host (4, 5) (fun r ->
                L.debug "Test RPC(4,5)" ;
                OUnit2.assert_equal r (Ok "9")))
end

let () =
    let module TcpConfig =
        struct
            include Rpc.DefaultTcpConfig
            let cnx_timeout = 0.2
            let max_accepted = Some 1
        end in
    let module R = RPC_Checks(Rpc.Tcp(TcpConfig)) in R.checks () ;
    Tcp_Checks.checks () ;
    Event.loop ()

