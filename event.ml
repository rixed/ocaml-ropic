open Batteries
open Ropic

type fd = Unix.file_descr

type monitored_files = fd list (* readables *)
                     * fd list (* writables *)
                     * fd list (* exceptional conditions *)

type handler = { register_files : monitored_files -> monitored_files ;
                  process_files : handler (* so we can unregister *) -> monitored_files -> unit }

type alert = float * (unit -> unit)

module AlertHeap = Heap.Make (
struct
    type t = alert
    let compare (a, _af) (b, _bf) = compare a b
end)
let alerts = ref AlertHeap.empty

exception Retry of string

module type S =
sig
  module L : Log.S
  val unregister : handler -> unit
  val register : handler -> unit
  val loop : ?timeout:float -> ?until:(unit -> bool) -> unit -> unit
  val register_timeout : (handler -> unit) -> unit
  val at : float -> (unit -> unit) -> unit
  val pause : float -> (unit -> unit) -> unit
  val condition : ?timeout:float -> (unit -> bool) -> (unit res -> unit) -> unit
  val clear : unit -> unit
  val retry_on_error : ?max_tries:int -> ?delay:float ->
                       ?max_delay:float -> ?geom_backoff:float ->
                       (('a res -> unit) -> unit) ->
                       ('a res -> unit) -> unit
  val forever : ?every:float -> ?variability:float ->
                ('a -> unit) -> 'a -> unit
end

module Make (L : Log.S) : S with module L = L =
struct
  module L = L
  type condition = float option (* deadline *)
                 * (unit -> bool) (* condition *)
                 * (unit res -> unit) (* continuation *)
  let conditions = ref ([] : condition list) (* test them each time anything happens *)

  let try_conditions now =
      (* Warning: Callbacks are allowed to queue new conditions *)
      let conds = !conditions in
      conditions := [] ;
      let rem_conds = List.fold_left (fun lst (d, c, f as cond) ->
          (* We have to check the condition before the timeout since we may
           * check the condition only once per second. *)
          try if c () then (f (Ok ()) ; lst) else (
            match d with
            | Some dl when now > dl -> f (Err "timeout") ; lst
            | _ -> cond::lst
          ) with exn ->
             L.error "condition or handler raised %s, ignoring" (Printexc.to_string exn) ;
             lst) [] conds in
      conditions := List.rev_append !conditions rem_conds

  let try_alerts () =
      (* take care that alert can add alerts and so on: *)
      let latest_alert = Unix.gettimeofday () +. 0.001 in
      let rec next_alert () =
          if AlertHeap.size !alerts > 0 then (
              let t, f = AlertHeap.find_min !alerts in
              if t < latest_alert then (
                  alerts := AlertHeap.del_min !alerts ; (* take care not to call anything between find_min and del_min! *)
                  (try f ()
                   with exn ->
                     L.error "some alert handler raised %s, ignoring" (Printexc.to_string exn)) ;
                  next_alert ())) in
      next_alert ()

  (* This can be changed (using register) at any point, esp. during file
   * descriptors gathering or processing. So we want to read the original
   * !handlers rather than pass its value to functions. *)
  let handlers = ref []

  let select_once max_timeout =
      let collect_all_monitored_files () =
          List.fold_left (fun files handler ->
              handler.register_files files)
              ([],[],[]) !handlers
      and process_all_monitored_files files =
          List.iter (fun handler ->
              try handler.process_files handler files
              with exn ->
                L.error "some file handler raised %s, ignoring" (Printexc.to_string exn)
            ) !handlers in

      (* alerts go first *)
      try_alerts () ;
      let now = Unix.gettimeofday () in
      try_conditions now ;
      let open Unix in
      let rfiles, wfiles, efiles = collect_all_monitored_files () in
      let timeout =
          if AlertHeap.size !alerts = 0 then max_timeout else
          let next_time, _ = AlertHeap.find_min !alerts in
          let dt = next_time -. now in
          min max_timeout (max 0. dt) (* since negative timeout mean wait forever *) in
      (* Notice: we timeout the select after a max_timeout so that handlers have a chance to implement timeouting *)
      let ll = List.length in
      L.debug "Selecting %d+%d+%d files..." (ll rfiles) (ll wfiles) (ll efiles) ;
      try let changed_files = select rfiles wfiles efiles timeout in
          let l (a,b,c) = ll a + ll b + ll c in
          L.debug "selected %d files out of %d" (l changed_files) (l (rfiles,wfiles,efiles)) ;
          (* then file handlers - notice that alerts handlers may have closed some of the fd but
           * that's ok because actually file processors could also close other fds. *)
          process_all_monitored_files changed_files
      with Unix_error (EINTR,_,_) -> ()

  let loop ?(timeout=1.) ?(until=fun () -> false) () =
      L.info "Entering event loop" ;
      let prev_sigpipe = Sys.(signal 13 Signal_ignore) in
      while not (until () || !handlers = [] && AlertHeap.size !alerts = 0) do
          select_once timeout ;
      done ;
      Sys.(set_signal 13 prev_sigpipe) ;
      L.info "Exiting event loop"

  let register handler =
      handlers := handler :: !handlers ;
      L.debug "Registering a new handler, now got %d" (List.length !handlers)

  (* TODO: give current time to timeout handlers *)
  let register_timeout f =
      let handler = { register_files = identity ;
                      process_files = fun handler _files -> f handler } in
      register handler

  let at date f =
      alerts := AlertHeap.add (date, f) !alerts

  let pause delay f =
      let date = Unix.gettimeofday () +. delay in
      at date f

  let condition ?timeout cond f =
      let deadline = Option.map (fun dt -> Unix.gettimeofday () +. dt) timeout in
      conditions := (deadline, cond, f)::!conditions

  let unregister handler =
      handlers := List.filter ((!=) handler) !handlers ;
      L.debug "Unregistering a handler, now got %d" (List.length !handlers)

  (* TODO: an unregister_files operation to ask clients to close what they can? *)
  let clear () =
      handlers := [] ;
      alerts := AlertHeap.empty ;
      conditions := []

  (* Wrap f so that it would retry a few times on errors *)
  let retry_on_error ?(max_tries=5) ?(delay=0.31) ?(max_delay=30.) ?(geom_backoff=3.)
                     may_fail cont =
    let rec retry nb_tries delay =
      let fail err =
        L.info "Failed with %s, will retry in %.2f seconds" err delay ;
        pause delay (fun () ->
          retry (nb_tries + 1) (min max_delay (delay *. geom_backoff))) in
      may_fail (function
      | Err err when nb_tries < max_tries -> fail err
      | _ as x -> try cont x with Retry err -> fail err) in
    retry 1 delay

  let rec forever ?(every=1.) ?(variability=0.5) f x =
    (try f x with exn ->
        L.error "function raised %s, ignoring" (Printexc.to_string exn)) ;
    let delay = every +. variability *. (Random.float 1. -. 0.5) in
    pause delay (fun () -> forever ~every ~variability f x)

end

(* This provides a way to handle a set of file descriptors on read/write
 * with a simple callback that will receives whatever is read plus a writer
 * to send an answer to. *)

module type IOType =
sig
    type to_write
    type to_read
    val serialize : to_write -> string
    val unserialize : string -> int -> int -> (int * to_read) option
end

module BufferedIO (T : IOType) (E : S) =
struct
    let try_write_buf buf fd =
        let str = Buffer.contents buf in
        E.L.debug "writing %d bytes into fd %a" (String.length str) Log.file fd ;
        let sz = Unix.single_write fd str 0 (String.length str) in
        Buffer.clear buf ;
        Buffer.add_substring buf str sz (String.length str - sz)

    type todo = Nope | ToDo | Done (* for deferred closes *)
    (* return true if we should close fd *)
    let try_read_value fd buf value_cb writer =
        (* Append what can be read from fd into buf ;
           notice that if more than 1500 bytes are available
           then the event loop will call us again at once *)
        let str = String.create 1500 in
        let sz = Unix.read fd str 0 (String.length str) in
        E.L.debug "Read %d bytes from file" sz ;
        if sz = 0 then (
            value_cb writer EndOfFile ;
            true
        ) else (
            Buffer.add_substring buf str 0 sz ;
            (* Read one value, apply value_cb to it, then returns the offset of next value.
             * Beware that we may have 0, 1 or more values in rbuf *)
            let rec read_next content ofs =
                let len = String.length content - ofs in
                E.L.debug "Still %d bytes to read from buffer" len ;
                (match T.unserialize content ofs len with
                | None -> ofs
                | Some (value_len, value) ->
                    assert (value_len <= len) ;
                    value_cb writer (Value value) ;
                    read_next content (ofs + value_len)) in
            let content = Buffer.contents buf in
            Buffer.clear buf ;
            let ofs = read_next content 0 in
            Buffer.add_substring buf content ofs (String.length content - ofs) ;
            false
        )

    let start infd outfd value_cb =
        let inbuf = Buffer.create 2000
        and outbuf = Buffer.create 2000
        and close_out = ref Nope and closed_in = ref false in
        let writer c =
            if !close_out = Done then
              (* This will happen each time the client keeps the writer for a delayed answer
                 and the connection is closed in the meantime. *)
              let bt = Printexc.get_backtrace () in
              E.L.debug "asked to write in a closed file from %s" bt
            else match c with
            | Write v ->
                T.serialize v |>
                Buffer.add_string outbuf
            | Close ->
                if !close_out = Nope then close_out := ToDo in
        let buffer_is_empty b = Buffer.length b = 0 in
        let register_files (rfiles, wfiles, efiles) =
            (if !closed_in then rfiles else infd :: rfiles),
            (if !close_out <> Done && not (buffer_is_empty outbuf) then outfd :: wfiles else wfiles),
            efiles in
        let process_files handler (rfiles, wfiles, _) =
            if List.mem infd rfiles then (
                assert (not !closed_in) ; (* otherwise we were not in the select fileset *)
                if try_read_value infd inbuf value_cb writer then (
                    E.L.debug "infd is closed by peer" ;
                    closed_in := true ;
                    if infd <> outfd then (
                        E.L.debug "Closing infd" ;
                        Unix.close infd
                    ) else (
                        assert (!close_out <> Done) ;
                        close_out := ToDo))) ;
            if List.mem outfd wfiles then (
                if not (buffer_is_empty outbuf) then
                    try try_write_buf outbuf outfd
                    with Unix.Unix_error (Unix.EPIPE, _, _) ->
                        E.L.debug "File was closed remotely" ;
                        close_out := ToDo) ;
            if !close_out = ToDo then (
                E.L.debug "Closing outfd" ;
                Unix.close outfd ;
                close_out := Done ;
                if infd = outfd then closed_in := true ;
                if !closed_in then E.unregister handler)
            in
        E.register { register_files ; process_files } ;
        (* Return the writer function *)
        writer
end

(* The CLIENT module implements the client side of a connected channel.
 * It's using the above BufferedIO module once per connection (and also
 * for listening file descriptors, in the case of TCP. *)

module type CLIENT =
sig
    module Types : IOType

    (* [client server_addr reader] connect to the given endpoint and send
     * to [reader] all read values accompanied by a writer function. It
     * also returns the (same) writer function. *)
    val client : Address.t ->
                 ((Types.to_write write_cmd -> unit) ->
                  Types.to_read read_result ->
                  unit) ->
                 (Types.to_write write_cmd -> unit)
end

module type CLIENT_MAKER =
    functor (Types : IOType)
            (E : S) ->
            CLIENT with module Types = Types

module TcpClient (T : IOType)
                 (E : S) = (*: CLIENT with module Types = T =*)
struct
    module Types = T
    module BIO = BufferedIO (Types) (E)

    let connect server =
        let open Unix in
        getaddrinfo server.Address.name (string_of_int server.Address.port) [AI_SOCKTYPE SOCK_STREAM ; AI_CANONNAME ] |>
        List.find_map (fun ai ->
            try let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
                Unix.connect sock ai.ai_addr ;
                E.L.info "Connected to %a" print_addrinfo ai ;
                Some sock
            with exn ->
                E.L.debug "Cannot connect: %s" (Printexc.to_string exn) ;
                None)

    let client server buf_reader =
        match connect server with
        | exception Not_found -> (* When all addresses failed *)
          failwith ("Cannot connect to "^ server.Address.name ^":"^ string_of_int server.Address.port)
        | fd -> BIO.start fd fd buf_reader
end

module type SERVER =
sig
    module Types : IOType
    (* [serve service callback] listens on the given service address and
     * start serving each query with [callback].
     * A shutdown function is returned that will stop the server from
     * accepting new connections when called. *)
    val serve : Address.t ->
                (Address.t ->
                 (Types.to_write write_cmd -> unit) ->
                 Types.to_read read_result -> unit) ->
                (unit -> unit)
end

module type SERVER_MAKER =
    functor (Types : IOType)
            (E : S) ->
            SERVER with module Types = Types

(* TODO: use proper sets rather than lists for fds *)
let list_union l1 l2 =
  List.fold_left (fun prev e1 ->
      if List.mem e1 l2 then e1::prev else prev
    ) [] l1

exception BadAddress

module TcpServer (T : IOType)
                 (E : S) : SERVER with module Types = T =
struct
    module Types = T
    module BIO = BufferedIO (Types) (E)

    let listen my_addr =
        let open Unix in
        getaddrinfo my_addr.Address.name (string_of_int my_addr.Address.port) [
            AI_SOCKTYPE SOCK_STREAM ; AI_PASSIVE ; AI_CANONNAME ] |>
        List.filter_map (fun ai ->
            try let sock = socket ai.ai_family ai.ai_socktype 0 in
                setsockopt sock SO_REUSEADDR true ;
                setsockopt sock SO_KEEPALIVE true ;
                bind sock ai.ai_addr ;
                listen sock 5 ;
                E.L.info "Listening on %a" print_addrinfo ai ;
                Some sock
            with exn ->
                E.L.debug "Cannot listen on %a: %s" print_addrinfo ai (Printexc.to_string exn) ;
                None)

    let serve my_addr value_cb =
        let buf_writers = ref [] in (* all writer function to client cnxs *)
        let listen_fds = listen my_addr in
        if listen_fds = [] then (
          E.L.error "Cannot bind address %a" Address.print my_addr ;
          raise BadAddress
        ) else
        let rec shutdown () =
            E.L.debug "Shutdown: Closing listening socket(s)" ;
            List.iter Unix.close listen_fds ;
            E.unregister handler ;
            E.L.debug "Shutdown: Close all cnxs to clients" ;
            List.iter (fun w -> w Close) !buf_writers ;
            buf_writers := []
        (* We need a special kind of event handler to handle the listener fd:
         * one that accept when it's readable and that never write. *)
        and register_files (rfiles, wfiles, efiles) =
            List.rev_append listen_fds rfiles, wfiles, efiles
        and process_files _handler (rfiles, _, _) =
            list_union listen_fds rfiles |>
            List.iter (fun listen_fd ->
                let client_fd, sockaddr = Unix.accept listen_fd in
                E.L.info "Accepting new cnx %a" Log.file client_fd ;
                let clt = Address.of_sockaddr sockaddr in
                buf_writers := BIO.start client_fd client_fd (value_cb clt) :: !buf_writers
            )
        and handler = { register_files ; process_files } in
        E.register handler ;
        shutdown
end

let max_pdu_size = 60000

module UdpClient (T : IOType)
                 (E : S) : CLIENT with module Types = T =
struct
    module Types = T

    let connect server =
      let open Unix in
      match
        getaddrinfo server.Address.name (string_of_int server.Address.port) [
          AI_SOCKTYPE SOCK_DGRAM ; AI_CANONNAME ] |>
        List.find_map (fun ai ->
          try let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
              connect sock ai.ai_addr ;
              E.L.debug "Will send datagrams to %a" print_addrinfo ai ;
              Some sock
          with exn ->
              E.L.debug "Cannot connect: %s" (Printexc.to_string exn) ;
              None) with
      | exception Not_found ->
        failwith ("Cannot resolve "^ server.Address.name ^":"^ string_of_int server.Address.port)
      | sock -> sock

    let client server value_cb =
        let sock = connect server in
        let closed = ref false in
        let rec dgram_writer = function
          | Close ->
            assert (not !closed) ; (* punish double close *)
            closed := true ;
            E.unregister handler
          | Write v ->
            assert (not !closed) ;
            let data = T.serialize v in
            let len = String.length data in
            E.L.debug "Send datagram of %d bytes" len ;
            let sz = Unix.send sock data 0 len [] in
            if sz <> len then E.L.error "Sent only %d bytes out of %d. I'm sooory!" sz len
        and register_files (rfiles, wfiles, efiles) =
          (* Given we send the datagrams directly in the writer, we
           * do not need to monitor sock for writing. *)
          (if !closed then rfiles else sock :: rfiles),
          wfiles, efiles
        and process_files =
          let recv_buffer = String.make max_pdu_size 'X' in
          fun _handler (rfiles, _, _) ->
            if List.mem sock rfiles then (
              let sz =
                let open Unix in
                try recv sock recv_buffer 0 max_pdu_size []
                with Unix_error (ECONNREFUSED, _, _) -> 0 in
              if sz = 0 then (
                E.L.warn "No (more) connection to server"
              ) else (
                match T.unserialize recv_buffer 0 sz with
                | None -> E.L.warn "Discarding datagram of %d bytes" sz
                | Some (sz', value) ->
                  if sz <> sz' then E.L.warn "Received garbage (%d bytes instead of %d)" sz' sz else
                  value_cb dgram_writer (Value value)
              )
            )
        and handler = { register_files ; process_files } in
        E.register handler ;
        dgram_writer
end

module UdpServer (T : IOType)
                 (E : S) : SERVER with module Types = T =
struct
    module Types = T

    let bind my_addr =
        let open Unix in
        E.L.info "Binding address %a" Address.print my_addr ;
        getaddrinfo my_addr.Address.name (string_of_int my_addr.Address.port) [
            AI_SOCKTYPE SOCK_DGRAM ; AI_PASSIVE ; AI_CANONNAME ] |>
        List.filter_map (fun ai ->
            try let sock = socket ai.ai_family ai.ai_socktype 0 in
                setsockopt sock SO_REUSEADDR true ;
                setsockopt sock SO_KEEPALIVE true ;
                bind sock ai.ai_addr ;
                E.L.info "Bound to %a" print_addrinfo ai ;
                Some sock
            with exn ->
                E.L.debug "Cannot bind to %a: %s" print_addrinfo ai (Printexc.to_string exn) ;
                None)

    let serve =
        let recv_buffer = String.make max_pdu_size 'X' in
        fun my_addr value_cb ->
          let socks = bind my_addr in
          if socks = [] then (
            E.L.error "Cannot bind address %a" Address.print my_addr ;
            raise BadAddress
          ) else
          let rec stop_listening () =
              E.L.debug "Closing server socket(s)" ;
              List.iter Unix.close socks ;
              E.unregister handler
          and dgram_writer sock dest_addr = function
            | Close -> ()
            | Write v ->
              let data = Types.serialize v in
              let len = String.length data in
              E.L.debug "Send datagram of %d bytes to %a" len print_sockaddr dest_addr ;
              let sz = Unix.sendto sock data 0 len [] dest_addr in
              if sz <> len then E.L.error "Sent only %d bytes out of %d. I'm sooory!" sz len
          and register_files (rfiles, wfiles, efiles) =
              List.rev_append socks rfiles, wfiles, efiles
          and process_files _handler (rfiles, _, _) =
            list_union socks rfiles |>
            List.iter (fun sock ->
                let sz, peer_addr = Unix.recvfrom sock recv_buffer 0 max_pdu_size [] in
                E.L.debug "Received a datagram of %d bytes from %a" sz print_sockaddr peer_addr ;
                (match Types.unserialize recv_buffer 0 sz with
                | None -> E.L.warn "Received %d bytes of garbage" sz
                | Some (sz', value) ->
                  if sz' <> sz then E.L.warn "Received %d bytes instead of %d" sz sz' else
                  value_cb (Address.of_sockaddr peer_addr) (dgram_writer sock peer_addr) (Value value)))
          and handler = { register_files ; process_files } in
          E.register handler ;
          stop_listening
end

