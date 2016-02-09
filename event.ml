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
    let compare (a,af) (b,bf) =
        match compare a b with 0 -> compare af bf | n -> n
end)
let alerts = ref AlertHeap.empty

type condition = (unit -> bool) * (unit -> unit)
module type S =
sig
  module L : Log.S
  val unregister : handler -> unit
  val register : handler -> unit
  val loop : ?timeout:float -> unit -> unit
  val register_timeout : (handler -> unit) -> unit
  val pause : float -> (unit -> unit) -> unit
  val condition : (unit -> bool) -> (unit -> unit) -> unit
  val clear : unit -> unit
end

module Make (L : Log.S) : S with module L = L =
struct
  module L = L
  let conditions = ref ([] : condition list) (* test them each time anything happens *)

  let try_conditions () =
      conditions := List.fold_left (fun lst (c, f as cond) ->
          if c () then (
              f () ;
              lst
          ) else cond::lst) [] !conditions

  let select_once max_timeout handlers =
      let collect_all_monitored_files handlers =
          List.fold_left (fun files handler ->
              handler.register_files files)
              ([],[],[]) handlers
      and process_all_monitored_files handlers files =
          List.iter (fun handler -> handler.process_files handler files) handlers in

      try_conditions () ;
      let open Unix in
      let rfiles, wfiles, efiles = collect_all_monitored_files handlers in
      let timeout =
          if AlertHeap.size !alerts = 0 then max_timeout else
          let next_time, _ = AlertHeap.find_min !alerts in
          let dt = next_time -. Unix.gettimeofday () in
          min max_timeout (max 0. dt) (* since negative timeout mean wait forever *) in
      (* Notice: we timeout the select after a max_timeout so that handlers have a chance to implement timeouting *)
      try let changed_files = select rfiles wfiles efiles timeout in
          (* alerts go first (take care that alert can add alerts and so on) *)
          let latest_alert = Unix.gettimeofday () +. 0.001 in
          let rec next_alert () =
              if AlertHeap.size !alerts > 0 then (
                  let t, f = AlertHeap.find_min !alerts in
                  if t < latest_alert then (
                      alerts := AlertHeap.del_min !alerts ; (* take care not to call anything between find_min and del_min! *)
                      f () ;
                      next_alert ())) in
          next_alert () ;
          (* then file handlers *)
          let ll = List.length in let l (a,b,c) = ll a + ll b + ll c in
          L.debug "selected %d files out of %d" (l changed_files) (l (rfiles,wfiles,efiles)) ;
          process_all_monitored_files handlers changed_files
      with Unix_error (EINTR,_,_) -> ()

  let handlers = ref []

  let loop ?(timeout=1.) () =
      L.info "Entering event loop" ;
      while not (!handlers = [] && AlertHeap.size !alerts = 0) do
          select_once timeout !handlers
      done

  let register handler =
      handlers := handler :: !handlers ;
      L.debug "Registering a new handler, now got %d" (List.length !handlers)

  (* TODO: give current time to timeout handlers *)
  let register_timeout f =
      let handler = { register_files = identity ;
                      process_files = fun handler _files -> f handler } in
      register handler

  let pause delay f =
      let date = Unix.gettimeofday () +. delay in
      alerts := AlertHeap.add (date, f) !alerts

  let condition cond f =
      conditions := (cond, f)::!conditions

  let unregister handler =
      handlers := List.filter ((!=) handler) !handlers ;
      L.debug "Unregistering a handler, now got %d" (List.length !handlers)

  let clear () =
      handlers := [] ;
      alerts := AlertHeap.empty ;
      conditions := []
end

(* Buffered reader/writer of marshaled values of type IOType.t_read/IOType.t_write. *)
module BufferedIO (T : IOType)
                  (Pdu : PDU with type BaseIOType.t_read = T.t_read and type BaseIOType.t_write = T.t_write)
                  (E : S) =
struct
    let try_write_buf buf fd =
        let str = Buffer.contents buf in
        E.L.debug "writing %d bytes info fd %a" (String.length str) Log.file fd ;
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
            value_cb writer T.EndOfFile ;
            true
        ) else (
            Buffer.add_substring buf str 0 sz ;
            (* Read one value, apply value_cb to it, then returns the offset of next value.
             * Beware that we may have 0, 1 or more values in rbuf *)
            let rec read_next content ofs =
                let len = String.length content - ofs in
                E.L.debug "Still %d bytes to read from buffer" len ;
                (* Do we have enough bytes to read a value? *)
                (match Pdu.has_value content ofs len with
                | None -> ofs
                | Some value_len ->
                    assert (value_len <= len) ;
                    Pdu.of_string content ofs value_len |>
                    Option.may (fun v -> value_cb writer (T.Value v)) ;
                    read_next content (ofs + value_len)) in
            let content = Buffer.contents buf in
            Buffer.clear buf ;
            let ofs = read_next content 0 in
            Buffer.add_substring buf content ofs (String.length content - ofs) ;
            false
        )

    let start ?cnx_timeout infd outfd value_cb =
        let last_used = ref 0. in (* for timeouting *)
        let reset_timeout_and f x =
            last_used := Unix.gettimeofday () ;
            f x in
        let inbuf = Buffer.create 2000
        and outbuf = Buffer.create 2000
        and close_out = ref Nope and closed_in = ref false in
        let writer c =
            assert (!close_out <> Done) ; (* otherwise why were we been given the possibility to write? *)
            match c with
            | T.Write v ->
                Pdu.to_string v |>
                Buffer.add_string outbuf
            | T.Close ->
                if !close_out = Nope then close_out := ToDo in
        let writer = reset_timeout_and writer in
        let buffer_is_empty b = Buffer.length b = 0 in
        let register_files (rfiles, wfiles, efiles) =
            (if !closed_in then rfiles else infd :: rfiles),
            (if !close_out <> Done && not (buffer_is_empty outbuf) then outfd :: wfiles else wfiles),
            efiles in
        let process_files handler (rfiles, wfiles, _) =
            if List.mem infd rfiles then (
                assert (not !closed_in) ; (* otherwise we were not in the select fileset *)
                if try_read_value infd inbuf (reset_timeout_and value_cb) writer then (
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
                    try_write_buf outbuf outfd) ;
            Option.may (fun cnx_timeout ->
                let now = Unix.gettimeofday () in
                if now -. !last_used > cnx_timeout then (
                    last_used := now ; (* reset cnx_timeout *)
                    value_cb writer (Timeout now)))
                cnx_timeout ;
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

module TcpClient (T : IOType)
                 (Pdu : PDU with type BaseIOType.t_read = T.t_read and type BaseIOType.t_write = T.t_write)
                 (E : S) :
sig
    (* [client "server.com" "http" reader] connect to server.com:80 and will send all read value to [reader]
     * function. The returned value is the writer function.
     * Notice that the reader is a callback, so this is different (and more complex) than the function call
     * abstraction. The advantage is that the main thread can do a remote call and proceed to something else
     * instead of being forced to wait for the response (event driven design). This also allows 0 or more than
     * 1 return values.
     * When timeouting, [reader] will be called with [EndOfFile] and input stream will be closed. You must
     * still close output stream if you want to close the socket for good. *)
    val client : ?cnx_timeout:float -> string -> string -> ((T.write_cmd -> unit) -> T.read_result -> unit) -> (T.write_cmd -> unit)
end =
struct
    module BIO = BufferedIO (T) (Pdu) (E)

    let connect host service =
        let open Unix in
        getaddrinfo host service [AI_SOCKTYPE SOCK_STREAM ; AI_CANONNAME ] |>
        List.find_map (fun ai ->
            E.L.info "Connecting to %s:%s" ai.ai_canonname service ;
            try let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
                Unix.connect sock ai.ai_addr ;
                Some sock
            with exn ->
                E.L.debug "Cannot connect: %s" (Printexc.to_string exn) ;
                None)

    let client ?cnx_timeout host service buf_reader =
        try let fd = connect host service in
            BIO.start ?cnx_timeout fd fd buf_reader
        with Not_found ->
            failwith ("Cannot connect to "^ host ^":"^ service)
end

module TcpServer (T : IOType)
                 (Pdu : PDU with type BaseIOType.t_read = T.t_read and type BaseIOType.t_write = T.t_write)
                 (Clt : sig type t end)
                 (E : S) :
sig
    (* [serve service new_clt callback] listen on port [service] and serve each query with [callback].
     * A shutdown function is returned that will stop the server from accepting new connections. *)
    val serve : ?cnx_timeout:float -> ?max_accepted:int -> string ->
                (Address.t -> Clt.t) ->
                (Clt.t -> (T.write_cmd -> unit) -> T.read_result -> unit) ->
                (unit -> unit)
end =
struct
    module BIO = BufferedIO (T) (Pdu) (E)

    let listen service =
        let open Unix in
        match getaddrinfo "" service [AI_SOCKTYPE SOCK_STREAM; AI_PASSIVE;] with
        | ai::_ ->
            E.L.info "Listening on %s" service ;
            let sock = socket PF_INET SOCK_STREAM 0 in
            setsockopt sock SO_REUSEADDR true ;
            setsockopt sock SO_KEEPALIVE true ;
            bind sock ai.ai_addr ;
            listen sock 5 ;
            sock
        | [] ->
            failwith ("Cannot listen to "^ service)

    let serve ?cnx_timeout ?max_accepted service new_clt value_cb =
        let accepted = ref 0 in
        Option.may (fun n -> assert (n > 0)) max_accepted ;
        let listen_fd = listen service in
        let rec stop_listening () =
            E.L.debug "Closing listening socket" ;
            Unix.close listen_fd ;
            E.unregister handler
        (* We need a special kind of event handler to handle the listener fd:
         * one that accept when it's readable and that never write. *)
        and register_files (rfiles, wfiles, efiles) =
            listen_fd :: rfiles, wfiles, efiles
        and process_files _handler (rfiles, _, _) =
            if List.mem listen_fd rfiles then (
                let client_fd, sockaddr = Unix.accept listen_fd in
                E.L.info "Accepting new cnx %a" Log.file client_fd ;
                incr accepted ;
                (match max_accepted with
                    | Some n when !accepted >= n -> stop_listening ()
                    | _ -> ()) ;
                let clt = new_clt (Address.of_sockaddr sockaddr) in
                let _buf_writer = BIO.start ?cnx_timeout client_fd client_fd (value_cb clt) in
                ()
            )
        and handler = { register_files ; process_files } in
        E.register handler ;
        stop_listening
end

