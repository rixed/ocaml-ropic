open Batteries
open Ropic

module L = Log.Make (Log.ToStdout)
module E = Event.Make (L.Debug)

(* This program merely:
 * listen to some port and connect to a given destination, then
 * forward what it receives to this destination, while introducing:
 * - some amount of random lag
 * - some amount of random loss
 * - some amount of random fuzzing
 *
 * These amounts are changing from time to time within given bounds,
 * for each individual links or set of links.
 *)

let random_duration () =
    Random.float 10.

(* ratio to percent *)
let r2p r = r *. 100.

module Setting =
struct
    type t =
        { mutable min : float ;
          mutable max : float }

    let print fmt t =
        Format.fprintf fmt "from %g%% to %g%%" (r2p t.min) (r2p t.max)

    let make min max =
        assert (min <= max) ;
        { min ; max }

    let make_ok () =
        { min = 0. ; max = 0. }

    let of_string str =
        let min, max = String.split str "-" in
        make (float_of_string min) (float_of_string max)
end

module Config =
struct
    type t =
        { mutable lag : Setting.t ;
          mutable loss : Setting.t ;
          mutable fuzz : Setting.t }

    let print fmt t =
        Format.fprintf fmt "lag: %a, loss: %a, fuzz: %a"
            Setting.print t.lag
            Setting.print t.loss
            Setting.print t.fuzz

    let make_ok () =
        { lag = Setting.make_ok () ;
          loss = Setting.make_ok () ;
          fuzz = Setting.make_ok () }

    let make lag loss fuzz =
        { lag ; loss ; fuzz }

    let of_string str =
        let t = make_ok () in
        String.nsplit str "," |>
        List.iter (fun s ->
            let n, v = String.split s "=" in
            let v = Setting.of_string v in
            match String.lowercase n with
            | "lag" -> t.lag <- v
            | "loss" -> t.loss <- v
            | "fuzz" -> t.fuzz <- v
            | _ -> invalid_arg n) ;
        t

end

module Degradation =
struct
    type t = { chance : float ; duration : float }

    let print fmt t =
        Format.fprintf fmt "%g%% chance for %gs" (r2p t.chance) t.duration

    let make setting =
        let open Setting in
        { chance = Random.float (setting.max -. setting.min) +. setting.min ;
          duration = random_duration () }

    let make_ok () =
        { chance = 0. ;
          duration = random_duration () }
end

module Condition =
struct
    type t =
        { lag : Degradation.t ;
          loss : Degradation.t ;
          fuzz : Degradation.t }

    let print fmt t =
        Format.fprintf fmt "lag: %a, loss: %a, fuzz: %a"
            Degradation.print t.lag
            Degradation.print t.loss
            Degradation.print t.fuzz

    let make conf =
        { lag = Degradation.make conf.Config.lag ;
          loss = Degradation.make conf.Config.loss ;
          fuzz = Degradation.make conf.Config.fuzz }

    let make_ok () =
        { lag = Degradation.make_ok () ;
          loss = Degradation.make_ok () ;
          fuzz = Degradation.make_ok () }

end

module Link =
struct
    module SrvTypes =
    struct
        type to_write = string
        type to_read = string
        include Pdu.Marshaler
    end
    module CltTypes =
    struct
        type to_write = string
        type to_read = string
        include Pdu.Marshaler
    end
    module Clt =
    struct
        type queue =
            { q : string write_cmd Queue.t ;
              mutable writer : (string write_cmd -> unit) option }
        let make_queue () =
            { q = Queue.create () ;
              writer = None }
        type t =
            { address : Address.t ;
              queues : queue * queue ;
              mutable condition : Condition.t * Condition.t ;
              mutable closed : bool }
    end
    module TcpServer = Event.TcpServer (SrvTypes) (E)
    module TcpClient = Event.TcpClient (CltTypes) (E)

    type t =
        (* We call 'out' the direction from client to server
         * and 'in' the direction from server to client, ie. we adopt
         * the client perspective. *)
        { listen_to : Address.t ;
          forward_to : Address.t ;
          mutable config : Config.t * Config.t (* in, out *) ;
          mutable clients : Clt.t list }

    let print fmt t =
        Format.fprintf fmt "%s:%d:%s:%d" t.listen_to.name t.listen_to.port t.forward_to.name t.forward_to.port

    type which = { f : 'a. 'a * 'a -> 'a }
    let w_fst = { f = fst }
    let w_snd = { f = snd }
    let rec delay_send name clt which =
        let queue = which.f clt.Clt.queues
        and cond = which.f clt.Clt.condition in
        (match queue.Clt.writer with
        | None -> ()
        | Some writer ->
            if not (Queue.is_empty queue.q) then (
                let x = Queue.take queue.q in
                E.L.debug "%s: Writing %s..." name (match x with Close -> "FIN" | Write _ -> "a string") ;
                writer x ;
                if x = Close then clt.closed <- true
            )) ;
        if not clt.closed then (
            let open Condition in
            let d = Random.float cond.lag.chance in
            E.L.debug "%s: rescheduling in %gs" name d ;
            E.pause d (fun () -> delay_send name clt which)
        )

    let make_client t address =
        let open Clt in
        E.L.debug "New client: %s" (Address.to_string address) ;
        let client =
            { address = address ;
              queues = make_queue (), make_queue () ;
              condition = Condition.make (fst t.config), Condition.make (snd t.config) ;
              closed = false } in
        t.clients <- client::t.clients ;
        delay_send "in"  client w_fst ;
        delay_send "out" client w_snd ;
        client

    let make listen_to_name listen_to_port host_name host_port conf =
        { listen_to = Address.make listen_to_name (int_of_string listen_to_port) ;
          forward_to = Address.make host_name (int_of_string host_port) ;
          config = conf, conf ;
          clients = [] }

    (* String format is similar to ssh port redirection format:
     * listen_to:remote_server:remote_port
     * It is created with a pristine config *)
    let of_string str =
        let h, s =
            try String.split str "," with Not_found -> str, "" in
        let conf = Config.of_string s in
        match String.nsplit h ":" with
        | [ listen_to_name ; listen_to_port ; host_name ; host_port ] ->
            make listen_to_name listen_to_port host_name host_port conf
        | [ listen_to_port ; host_port] ->
            make "localhost" listen_to_port "localhost" host_port conf
        | _ ->
            invalid_arg str

    let start t =
        let server_to_client clt _writer = function
            | Value str ->
                Queue.add (Write str) (fst clt.Clt.queues).q
            | EndOfFile ->
                Queue.add Close (fst clt.Clt.queues).q in
        let client_to_server clt_addr to_client v =
            let clt = make_client t clt_addr in
            (* We can't know the to_client writer before some client actually
             * connect. So we save it now. *)
            (match (fst clt.Clt.queues).Clt.writer with
            | Some _ -> ()
            | None ->
                (fst clt.Clt.queues).Clt.writer <- Some to_client ;
                (* Now start a new connection to the server for this client *)
                let to_server =
                    TcpClient.client t.forward_to (server_to_client clt) in
                (snd clt.queues).Clt.writer <- Some to_server) ;
            match v with
            | Value str ->
                Queue.add (Write str) (snd clt.Clt.queues).q
            | EndOfFile ->
                Queue.add Close (snd clt.Clt.queues).q in
        let shutdown = TcpServer.serve t.listen_to client_to_server in
        shutdown
end

let () =
    let open Cmdliner in
    let link_parser str =
        try `Ok (Link.of_string str)
        with (Invalid_argument s) -> `Error s in
    let link_converter = link_parser, Link.print in
    let links =
        let doc =
            "port forward description, a la SSH: local_host:local_port:remote_host:remote_port "^
            "followed by optional settings (lag, loss, fuzz)" in
        Arg.(non_empty (pos_all link_converter [] (info ~doc ~docv:"LINK" []))) in
    let start_all links =
        List.map Link.start links in
    match Term.(eval (pure start_all $ links, info "fungw")) with
    | `Error _ -> exit 1
    | `Ok _shutdowns ->
        E.loop () ;
        exit 0
    | `Version | `Help -> exit 0

