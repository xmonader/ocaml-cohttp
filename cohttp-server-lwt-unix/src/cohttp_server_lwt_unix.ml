(*{{{ Copyright (c) 2012-2014 Anil Madhavapeddy <anil@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
  }}}*)

open Lwt.Syntax

module Body = struct
  module Substring = struct
    type t = { base : string; pos : int; len : int }
  end

  module Encoding = struct
    type t = Fixed of int64 | Chunked

    let fixed i = Fixed i
    let chunked = Chunked
  end

  type t =
    Encoding.t
    * [ `String of string | `Stream of unit -> Substring.t option Lwt.t ]

  let encoding = fst

  let is_empty (_, body) =
    match body with `String s -> String.length s = 0 | `Stream _ -> false

  let string ?encoding s =
    let encoding =
      match encoding with
      | Some s -> s
      | None -> Encoding.Fixed (Int64.of_int (String.length s))
    in
    (encoding, `String s)

  let stream ?(encoding = Encoding.Chunked) f : t = (encoding, `Stream f)
  let chunk_size = 4096

  let write_chunk oc (sub : Substring.t) =
    let* () = Lwt_io.write oc (Printf.sprintf "%x\r\n" sub.len) in
    let* () = Lwt_io.write_from_string_exactly oc sub.base sub.pos sub.len in
    Lwt_io.write oc "\r\n"

  let next_chunk base ~pos =
    let len = String.length base in
    if pos >= len then None
    else Some { Substring.base; pos; len = min chunk_size (len - pos) }

  let rec write_string_as_chunks oc s ~pos =
    match next_chunk s ~pos with
    | None -> Lwt_io.write oc "\r\n"
    | Some chunk ->
        let* () = write_chunk oc chunk in
        let pos = pos + chunk.len in
        write_string_as_chunks oc s ~pos

  let rec write_fixed_stream oc f =
    let* chunk = f () in
    match chunk with
    | None -> Lwt.return_unit
    | Some { Substring.base; pos; len } ->
        let* () = Lwt_io.write_from_string_exactly oc base pos len in
        write_fixed_stream oc f

  let rec write_chunks_stream oc f =
    let* chunk = f () in
    match chunk with
    | None -> Lwt_io.write oc "\r\n"
    | Some chunk ->
        let* () = write_chunk oc chunk in
        write_chunks_stream oc f

  let write ((encoding, body) : t) oc =
    match body with
    | `String s -> (
        match encoding with
        | Fixed _ -> Lwt_io.write oc s
        | Chunked -> write_string_as_chunks oc s ~pos:0)
    | `Stream f -> (
        match encoding with
        | Fixed _ -> write_fixed_stream oc f
        | Chunked -> write_chunks_stream oc f)
end

module Input_channel = struct
  open Lwt.Infix
  module Bytebuffer = Cohttp_lwt.Private.Bytebuffer

  type t = { buf : Bytebuffer.t; chan : Lwt_io.input_channel }

  let refill ic buf ~pos ~len =
    Lwt.catch
      (fun () ->
        Lwt_io.read_into ic buf pos len >|= fun c ->
        if c > 0 then `Ok c else `Eof)
      (function Lwt_io.Channel_closed _ -> Lwt.return `Eof | exn -> raise exn)

  let create ?(buf_len = 0x4000) chan =
    { buf = Bytebuffer.create buf_len; chan }

  let read_line_opt t = Bytebuffer.read_line t.buf (refill t.chan)
  let read t count = Bytebuffer.read t.buf (refill t.chan) count
  let refill t = Bytebuffer.refill t.buf (refill t.chan)

  let with_input_buffer t ~f =
    let buf = Bytebuffer.unsafe_buf t.buf in
    let pos = Bytebuffer.pos t.buf in
    let len = Bytebuffer.length t.buf in
    let res, consumed = f (Bytes.unsafe_to_string buf) ~pos ~len in
    Bytebuffer.drop t.buf consumed;
    res
end

module IO = struct
  exception IO_error of exn

  let () =
    Printexc.register_printer (function
      | IO_error e -> Some ("IO error: " ^ Printexc.to_string e)
      | _ -> None)

  type 'a t = 'a Lwt.t

  let ( >>= ) = Lwt.bind
  let return = Lwt.return

  type ic = Input_channel.t
  type oc = Lwt_io.output_channel
  type conn = unit

  let wrap_read f ~if_closed =
    (* TODO Use [Lwt_io.is_closed] when available:
       https://github.com/ocsigen/lwt/pull/635 *)
    Lwt.catch f (function
      | Lwt_io.Channel_closed _ -> Lwt.return if_closed
      | Unix.Unix_error _ as e -> Lwt.fail (IO_error e)
      | exn -> raise exn)

  let wrap_write f =
    Lwt.catch f (function
      | Unix.Unix_error _ as e -> Lwt.fail (IO_error e)
      | exn -> raise exn)

  let read_line ic =
    wrap_read ~if_closed:None (fun () ->
        Input_channel.read_line_opt ic >>= function
        | None -> Lwt.return_none
        | Some _ as x -> Lwt.return x)

  let read ic count =
    let count = min count Sys.max_string_length in
    wrap_read ~if_closed:"" (fun () ->
        Input_channel.read ic count >>= fun buf -> Lwt.return buf)

  let refill ic = Input_channel.refill ic
  let with_input_buffer ic = Input_channel.with_input_buffer ic
  let write oc buf = wrap_write @@ fun () -> Lwt_io.write oc buf
  let flush oc = wrap_write @@ fun () -> Lwt_io.flush oc
end

module Request_io = Cohttp.Request.Private.Make (IO)
module Response_io = Cohttp.Response.Private.Make (IO)

module Context = struct
  type request_body = Unread | Reading of unit Lwt.t

  type t = {
    request : Http.Request.t;
    ic : Input_channel.t;
    oc : Lwt_io.output_channel;
    mutable request_body : request_body;
    response_sent : Http.Response.t Lwt.t;
    response_send : Http.Response.t Lwt.u;
  }

  let request t = t.request

  let create request ic oc =
    let response_sent, response_send = Lwt.wait () in
    { request; ic; oc; response_sent; response_send; request_body = Unread }

  let with_body t ~init ~f =
    assert (t.request_body = Unread);
    match Cohttp.Request.has_body t.request with
    | `Unknown | `No ->
        t.request_body <- Reading Lwt.return_unit;
        Lwt.return init
    | `Yes ->
        let rt, ru = Lwt.wait () in
        t.request_body <- Reading rt;
        let reader = Request_io.make_body_reader t.request t.ic in
        let rec loop acc =
          let* chunk = Request_io.read_body_chunk reader in
          let continue, acc =
            match chunk with
            | Chunk s -> (true, f s acc)
            | Final_chunk s -> (false, f s acc)
            | Done -> (false, acc)
          in
          if continue then loop acc else Lwt.return acc
        in
        let+ body = loop init in
        Lwt.wakeup_later ru ();
        body

  let read_body t =
    let+ buf =
      with_body t ~init:(Buffer.create 128) ~f:(fun chunk acc ->
          Buffer.add_string acc chunk;
          acc)
    in
    Buffer.contents buf

  let discard_body t = with_body t ~init:() ~f:(fun _ () -> ())
  let allowed_body _ _ = true

  let respond t (response : Http.Response.t) (body : Body.t) =
    let allowed_body = allowed_body t response in
    if (not allowed_body) && Body.is_empty body then
      invalid_arg "body must be empty";
    let headers =
      match allowed_body with
      | false -> response.headers
      | true ->
          let encoding =
            match (Body.encoding body : Body.Encoding.t) with
            | Fixed i -> Http.Transfer.Fixed i
            | Chunked -> Chunked
          in
          Http.Header.add_transfer_encoding response.headers encoding
    in
    let* () =
      let* () = Lwt_io.write t.oc (Http.Version.to_string response.version) in
      let* () = Lwt_io.write_char t.oc ' ' in
      let* () = Lwt_io.write t.oc (Http.Status.to_string response.status) in
      let* () = Lwt_io.write t.oc "\r\n" in
      let* () =
        Http.Header.to_list headers
        |> Lwt_list.iter_s (fun (k, v) ->
               let* () = Lwt_io.write t.oc k in
               let* () = Lwt_io.write t.oc ": " in
               let* () = Lwt_io.write t.oc v in
               Lwt_io.write t.oc "\r\n")
      in
      let* () = Lwt_io.write t.oc "\r\n" in
      Body.write body t.oc
    in
    Lwt.wakeup_later t.response_send response;
    Lwt_io.flush t.oc
end

type on_exn = Hook | Callback of (exn -> unit)
type t = { callback : Context.t -> unit Lwt.t; on_exn : on_exn }

let create ?on_exn callback =
  let on_exn = match on_exn with None -> Hook | Some f -> Callback f in
  { on_exn; callback }

let rec read_request ic =
  let result =
    IO.with_input_buffer ic ~f:(fun buf ~pos ~len ->
        match Http.Private.Parser.parse_request ~pos ~len buf with
        | Ok (req, consumed) -> (`Ok req, consumed)
        | Error Partial -> (`Partial, 0)
        | Error (Msg msg) -> (`Invalid msg, 0))
  in
  match result with
  | `Partial -> (
      let* res = IO.refill ic in
      match res with `Ok -> read_request ic | `Eof -> Lwt.return `Eof)
  | `Ok req -> Lwt.return (`Ok req)
  | `Invalid msg -> Lwt.return (`Error msg)

let handle_connection { callback; on_exn } (ic, oc) =
  let ic = Input_channel.create ic in
  let on_exn =
    match on_exn with
    | Hook -> fun exn -> !Lwt.async_exception_hook exn
    | Callback f -> f
  in
  let rec loop callback ic oc =
    let* req = read_request ic in
    match req with
    | `Error _ | `Eof -> Lwt.return_unit
    | `Ok req ->
        let context = Context.create req ic oc in
        Lwt.dont_wait (fun () -> callback context) on_exn;
        let* response =
          match context.request_body with
          | Unread -> assert false (* TODO *)
          | Reading body ->
              let+ (), response = Lwt.both body context.response_sent in
              response
        in
        let keep_alive =
          Http.Request.is_keep_alive req
          &&
          match Http.Header.connection (Http.Response.headers response) with
          | Some `Close -> false
          | Some `Keep_alive -> true
          | Some (`Unknown _) -> false
          | None -> Http.Response.version response = `HTTP_1_1
        in
        if keep_alive then loop callback ic oc else Lwt.return_unit
  in
  loop callback ic oc
