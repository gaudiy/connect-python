"""Provides a ConnectClient class for handling connections using the Connect protocol."""

import asyncio
import contextlib
import json
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Mapping,
)
from http import HTTPMethod, HTTPStatus
from typing import Any

import httpcore
from yarl import URL

from connect.byte_stream import HTTPCoreResponseAsyncByteStream
from connect.code import Code
from connect.codec import Codec, StableCodec
from connect.compression import COMPRESSION_IDENTITY, Compression, get_compresion_from_name
from connect.connect import (
    Address,
    Peer,
    Spec,
    StreamingClientConn,
    StreamType,
    ensure_single,
)
from connect.error import ConnectError
from connect.headers import Headers, include_request_headers
from connect.idempotency_level import IdempotencyLevel
from connect.protocol import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    PROTOCOL_CONNECT,
    ProtocolClient,
    ProtocolClientParams,
    code_from_http_status,
)
from connect.protocol_connect.constants import (
    CONNECT_HEADER_PROTOCOL_VERSION,
    CONNECT_HEADER_TIMEOUT,
    CONNECT_PROTOCOL_VERSION,
    CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
    CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
    CONNECT_STREAMING_HEADER_COMPRESSION,
    CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION,
    CONNECT_UNARY_HEADER_COMPRESSION,
    CONNECT_UNARY_TRAILER_PREFIX,
    DEFAULT_CONNECT_USER_AGENT,
)
from connect.protocol_connect.content_type import (
    connect_codec_from_content_type,
    connect_content_type_from_codec_name,
    connect_validate_unary_response_content_type,
)
from connect.protocol_connect.error_json import error_from_json
from connect.protocol_connect.marshaler import ConnectStreamingMarshaler, ConnectUnaryRequestMarshaler
from connect.protocol_connect.unmarshaler import ConnectStreamingUnmarshaler, ConnectUnaryUnmarshaler
from connect.session import AsyncClientSession
from connect.utils import (
    map_httpcore_exceptions,
)

EventHook = Callable[..., Any]


class ConnectClient(ProtocolClient):
    """ConnectClient is a client for handling connections using the Connect protocol.

    Attributes:
        params (ProtocolClientParams): Parameters for the protocol client.
        _peer (Peer): The peer object representing the connection endpoint.

    """

    params: ProtocolClientParams
    _peer: Peer

    def __init__(self, params: ProtocolClientParams) -> None:
        """Initialize the ProtocolConnect instance with the given parameters.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the ProtocolConnect instance.

        """
        self.params = params
        self._peer = Peer(
            address=Address(host=params.url.host or "", port=params.url.port or 80),
            protocol=PROTOCOL_CONNECT,
            query={},
        )

    @property
    def peer(self) -> Peer:
        """Return the peer associated with this instance.

        :return: The peer associated with this instance.
        :rtype: Peer
        """
        return self._peer

    def write_request_headers(self, stream_type: StreamType, headers: Headers) -> None:
        """Write the necessary request headers to the provided headers dictionary.

        This method ensures that the headers dictionary contains the required headers
        for a request, including user agent, protocol version, content type, and
        optionally, compression settings.

        Args:
            stream_type (StreamType): The type of stream for the request.
            headers (Headers): The dictionary of headers to be updated.

        Returns:
            None

        """
        if headers.get(HEADER_USER_AGENT, None) is None:
            headers[HEADER_USER_AGENT] = DEFAULT_CONNECT_USER_AGENT

        headers[CONNECT_HEADER_PROTOCOL_VERSION] = CONNECT_PROTOCOL_VERSION
        headers[HEADER_CONTENT_TYPE] = connect_content_type_from_codec_name(stream_type, self.params.codec.name)

        accept_compression_header = CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION
        if stream_type != StreamType.Unary:
            headers[CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION] = COMPRESSION_IDENTITY
            accept_compression_header = CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION
            if self.params.compression_name and self.params.compression_name != COMPRESSION_IDENTITY:
                headers[CONNECT_STREAMING_HEADER_COMPRESSION] = self.params.compression_name

        if self.params.compressions:
            headers[accept_compression_header] = ", ".join(c.name for c in self.params.compressions)

    def conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Establish a unary client connection with the given specifications and headers.

        Args:
            spec (Spec): The specification for the connection.
            headers (Headers): The headers to be included in the request.

        Returns:
            UnaryClientConn: The established unary client connection.

        """
        conn: StreamingClientConn
        if spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryClientConn(
                session=self.params.session,
                spec=spec,
                peer=self.peer,
                url=self.params.url,
                compressions=self.params.compressions,
                request_headers=headers,
                marshaler=ConnectUnaryRequestMarshaler(
                    codec=self.params.codec,
                    compression=get_compresion_from_name(self.params.compression_name, self.params.compressions),
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    headers=headers,
                ),
                unmarshaler=ConnectUnaryUnmarshaler(
                    codec=self.params.codec,
                    read_max_bytes=self.params.read_max_bytes,
                ),
            )
            if spec.idempotency_level == IdempotencyLevel.NO_SIDE_EFFECTS:
                conn.marshaler.enable_get = self.params.enable_get
                conn.marshaler.url = self.params.url
                if isinstance(self.params.codec, StableCodec):
                    conn.marshaler.stable_codec = self.params.codec
        else:
            conn = ConnectStreamingClientConn(
                session=self.params.session,
                spec=spec,
                peer=self.peer,
                url=self.params.url,
                codec=self.params.codec,
                compressions=self.params.compressions,
                request_headers=headers,
                marshaler=ConnectStreamingMarshaler(
                    codec=self.params.codec,
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    compression=get_compresion_from_name(self.params.compression_name, self.params.compressions),
                ),
                unmarshaler=ConnectStreamingUnmarshaler(
                    codec=self.params.codec,
                    read_max_bytes=self.params.read_max_bytes,
                ),
            )

        return conn


class ConnectUnaryClientConn(StreamingClientConn):
    """A client connection for unary RPCs using the Connect protocol.

    Attributes:
        _spec (Spec): The specification for the connection.
        _peer (Peer): The peer information.
        url (URL): The URL for the connection.
        compressions (list[Compression]): List of supported compressions.
        marshaler (ConnectUnaryRequestMarshaler): The marshaler for requests.
        unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler for responses.
        response_content (bytes | None): The content of the response.
        _response_headers (Headers): The headers of the response.
        _response_trailers (Headers): The trailers of the response.
        _request_headers (Headers): The headers of the request.
        _event_hooks (dict[str, list[EventHook]]): Event hooks for request and response.

    """

    session: AsyncClientSession
    _spec: Spec
    _peer: Peer
    url: URL
    compressions: list[Compression]
    marshaler: ConnectUnaryRequestMarshaler
    unmarshaler: ConnectUnaryUnmarshaler
    response_content: bytes | None
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers

    def __init__(
        self,
        session: AsyncClientSession,
        spec: Spec,
        peer: Peer,
        url: URL,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: ConnectUnaryRequestMarshaler,
        unmarshaler: ConnectUnaryUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initialize the ConnectProtocol instance.

        Args:
            session (AsyncClientSession): The session for the connection.
            spec (Spec): The specification for the connection.
            peer (Peer): The peer information.
            url (URL): The URL for the connection.
            compressions (list[Compression]): List of compression methods.
            request_headers (Headers): The headers for the request.
            marshaler (ConnectUnaryRequestMarshaler): The marshaler for the request.
            unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler for the response.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Event hooks for request and response. Defaults to None.

        Returns:
            None

        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.session = session
        self._spec = spec
        self._peer = peer
        self.url = url
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_content = None
        self._response_headers = Headers()
        self._response_trailers = Headers()
        self._request_headers = request_headers
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    @property
    def spec(self) -> Spec:
        """Return the specification of the protocol.

        Returns:
            Spec: The specification object of the protocol.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer object associated with this instance.

        :return: The peer object.
        :rtype: Peer
        """
        return self._peer

    async def _receive_messages(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives and unmarshals a message, yielding the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Yields:
            Any: The unmarshaled object.

        """
        obj = await self.unmarshaler.unmarshal(message)
        yield obj

    def receive(self, message: Any, _abort_event: asyncio.Event | None) -> AsyncIterator[Any]:
        """Receives a message and returns an asynchronous iterator over the processed message.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            AsyncIterator[Any]: An asynchronous iterator yielding processed message(s).

        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Retrieve the request headers.

        Returns:
            Headers: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    def on_request_send(self, fn: EventHook) -> None:
        """Register a callback function to be called when a request is sent.

        Args:
            fn (EventHook): The callback function to be registered. This function
                            will be called with the request details when a request
                            is sent.

        """
        self._event_hooks["request"].append(fn)

    async def send(
        self, messages: AsyncIterable[Any], timeout: float | None, abort_event: asyncio.Event | None
    ) -> None:
        """Send a single message asynchronously using either HTTP GET or POST, with support for timeouts and request abortion.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable yielding the message(s) to send. Only a single message is allowed.
            timeout (float | None): Optional timeout in seconds for the request. If provided, sets a read timeout for the request.
            abort_event (asyncio.Event | None): Optional asyncio event that, if set, aborts the request.

        Raises:
            ConnectError: If the request is aborted before or during execution, or if other connection errors occur.

        Side Effects:
            - Modifies request headers for timeout and content length as needed.
            - Invokes registered request and response event hooks.
            - Sets the unmarshaler's stream to the response stream for further processing.
            - Validates the response after receiving it.

        Notes:
            - If `marshaler.enable_get` is True, sends the request as HTTP GET; otherwise, uses HTTP POST.
            - Handles cancellation and cleanup if the abort event is triggered during the request.

        """
        extensions = {}
        if timeout:
            extensions["timeout"] = {"read": timeout}
            self._request_headers[CONNECT_HEADER_TIMEOUT] = str(int(timeout * 1000))

        message = await ensure_single(messages)
        data = self.marshaler.marshal(message)

        if self.marshaler.enable_get:
            if self.marshaler.url is None:
                raise ConnectError("url is not set", Code.INTERNAL)

            request = httpcore.Request(
                method=HTTPMethod.GET,
                url=httpcore.URL(
                    scheme=self.marshaler.url.scheme,
                    host=self.marshaler.url.host or "",
                    port=self.marshaler.url.port,
                    target=self.marshaler.url.raw_path_qs,
                ),
                headers=list(
                    include_request_headers(
                        headers=self._request_headers, url=self.url, content=data, method=HTTPMethod.GET
                    ).items()
                ),
                extensions=extensions,
            )
        else:
            self._request_headers[HEADER_CONTENT_LENGTH] = str(len(data))

            request = httpcore.Request(
                method=HTTPMethod.POST,
                url=httpcore.URL(
                    scheme=self.url.scheme,
                    host=self.url.host or "",
                    port=self.url.port,
                    target=self.url.raw_path,
                ),
                headers=list(
                    include_request_headers(
                        headers=self._request_headers, url=self.url, content=data, method=HTTPMethod.POST
                    ).items()
                ),
                content=data,
                extensions=extensions,
            )

        for hook in self._event_hooks["request"]:
            hook(request)

        with map_httpcore_exceptions():
            if not abort_event:
                response = await self.session.pool.handle_async_request(request=request)
            else:
                request_task = asyncio.create_task(self.session.pool.handle_async_request(request=request))
                abort_task = asyncio.create_task(abort_event.wait())

                done, _ = await asyncio.wait({request_task, abort_task}, return_when=asyncio.FIRST_COMPLETED)

                if abort_task in done:
                    request_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await request_task

                    raise ConnectError("request aborted", Code.CANCELED)

                abort_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await abort_task

                response = await request_task

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = HTTPCoreResponseAsyncByteStream(response.stream)

        await self._validate_response(response)

    @property
    def response_headers(self) -> Headers:
        """Return the response headers.

        Returns:
            Headers: A dictionary-like object containing the response headers.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Return the response trailers.

        Response trailers are additional headers sent after the response body.

        Returns:
            Headers: A dictionary containing the response trailers.

        """
        return self._response_trailers

    async def _validate_response(self, response: httpcore.Response) -> None:
        self._response_headers.update(Headers(response.headers))

        for key, value in self._response_headers.items():
            if not key.startswith(CONNECT_UNARY_TRAILER_PREFIX.lower()):
                self._response_headers[key] = value
                continue

            self._response_trailers[key[len(CONNECT_UNARY_TRAILER_PREFIX) :]] = value

        validate_error = connect_validate_unary_response_content_type(
            self.marshaler.codec.name if self.marshaler.codec else "",
            response.status,
            self._response_headers.get(HEADER_CONTENT_TYPE, ""),
        )

        compression = self._response_headers.get(CONNECT_UNARY_HEADER_COMPRESSION, None)
        if (
            compression
            and compression != COMPRESSION_IDENTITY
            and not any(c.name == compression for c in self.compressions)
        ):
            raise ConnectError(
                f"unknown encoding {compression}: accepted encodings are {', '.join(c.name for c in self.compressions)}",
                Code.INTERNAL,
            )

        self.unmarshaler.compression = get_compresion_from_name(compression, self.compressions)

        if validate_error:

            def json_ummarshal(data: bytes, _message: Any) -> Any:
                return json.loads(data)

            try:
                data = await self.unmarshaler.unmarshal_func(None, json_ummarshal)
                wire_error = error_from_json(data, validate_error)
            except ConnectError as e:
                raise e
            except Exception as e:
                raise ConnectError(
                    f"HTTP {response.status}",
                    code_from_http_status(response.status),
                ) from e

            wire_error.metadata = self._response_headers.copy()
            wire_error.metadata.update(self._response_trailers)
            raise wire_error

    @property
    def event_hooks(self) -> dict[str, list[EventHook]]:
        """Return the event hooks.

        This method returns a dictionary where the keys are strings representing
        event names, and the values are lists of EventHook objects associated with
        those events.

        Returns:
            dict[str, list[EventHook]]: A dictionary mapping event names to lists
            of EventHook objects.

        """
        return self._event_hooks

    @event_hooks.setter
    def event_hooks(self, event_hooks: dict[str, list[EventHook]]) -> None:
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    async def aclose(self) -> None:
        """Asynchronously closes the connection or releases any resources held by the object.

        This method should be called when the object is no longer needed to ensure proper cleanup.
        Currently, this implementation does not perform any actions, but it can be overridden in subclasses.

        Returns:
            None

        """
        return


class ConnectStreamingClientConn(StreamingClientConn):
    """ConnectStreamingClientConn is a class that manages a streaming client connection using the Connect protocol."""

    _spec: Spec
    _peer: Peer
    url: URL
    codec: Codec
    compressions: list[Compression]
    marshaler: ConnectStreamingMarshaler
    unmarshaler: ConnectStreamingUnmarshaler
    response_content: bytes | None
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers

    def __init__(
        self,
        session: AsyncClientSession,
        spec: Spec,
        peer: Peer,
        url: URL,
        codec: Codec,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: ConnectStreamingMarshaler,
        unmarshaler: ConnectStreamingUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initialize a new instance of the class.

        Args:
            session (AsyncClientSession): The session object for the connection.
            spec (Spec): The specification object.
            peer (Peer): The peer object.
            url (URL): The URL for the connection.
            codec (Codec): The codec to be used for encoding and decoding.
            compressions (list[Compression]): List of compression methods.
            request_headers (Headers): The headers for the request.
            marshaler (ConnectStreamingMarshaler): The marshaler for streaming.
            unmarshaler (ConnectStreamingUnmarshaler): The unmarshaler for streaming.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Event hooks for request and response. Defaults to None.

        Returns:
            None

        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.session = session
        self._spec = spec
        self._peer = peer
        self.url = url
        self.codec = codec
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_content = None
        self._response_headers = Headers()
        self._response_trailers = Headers()
        self._request_headers = request_headers
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    @property
    def spec(self) -> Spec:
        """Return the specification of the protocol.

        Returns:
            Spec: The specification object of the protocol.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer object associated with this instance.

        :return: The peer object.
        :rtype: Peer
        """
        return self._peer

    @property
    def request_headers(self) -> Headers:
        """Retrieve the request headers.

        Returns:
            Headers: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    @property
    def response_headers(self) -> Headers:
        """Return the response headers.

        Returns:
            Headers: A dictionary-like object containing the response headers.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Return the response trailers.

        Response trailers are additional headers sent after the response body.

        Returns:
            Headers: A dictionary containing the response trailers.

        """
        return self._response_trailers

    def on_request_send(self, fn: EventHook) -> None:
        """Register a callback function to be called when a request is sent.

        Args:
            fn (EventHook): The callback function to be registered. This function
                            will be called with the request details when a request
                            is sent.

        """
        self._event_hooks["request"].append(fn)

    async def receive(self, message: Any, abort_event: asyncio.Event | None = None) -> AsyncIterator[Any]:
        """Asynchronously receives and processes a message.

        Args:
            message (Any): The message to be processed.
            abort_event (asyncio.Event | None): Event to signal abortion of the operation.

        Yields:
            Any: Objects obtained from unmarshaling the message.

        Raises:
            ConnectError: If stream is malformed or aborted.

        """
        end_stream_received = False

        async for obj, end in self.unmarshaler.unmarshal(message):
            if abort_event and abort_event.is_set():
                raise ConnectError("receive operation aborted", Code.CANCELED)

            if end:
                if end_stream_received:
                    raise ConnectError("received extra end stream message", Code.INVALID_ARGUMENT)

                end_stream_received = True
                error = self.unmarshaler.end_stream_error
                if error:
                    for key, value in self.response_headers.items():
                        error.metadata[key] = value
                        error.metadata.update(self.unmarshaler.trailers.copy())
                    raise error

                for key, value in self.unmarshaler.trailers.items():
                    self.response_trailers[key] = value

                continue

            if end_stream_received:
                raise ConnectError("received message after end stream", Code.INVALID_ARGUMENT)

            yield obj

        if not end_stream_received:
            raise ConnectError("missing end stream message", Code.INVALID_ARGUMENT)

    async def send(
        self, messages: AsyncIterable[Any], timeout: float | None, abort_event: asyncio.Event | None
    ) -> None:
        """Send an asynchronous HTTP POST request with the given messages and handle the response.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be sent.
            timeout (float | None): Optional timeout value in seconds for the request. If provided,
                it sets the read timeout for the request.
            abort_event (asyncio.Event | None): Optional asyncio event that, if set, will abort the request.

        Raises:
            ConnectError: If the request is aborted or if there is an error during the request.

        Hooks:
            - Executes hooks registered in `self._event_hooks["request"]` before sending the request.
            - Executes hooks registered in `self._event_hooks["response"]` after receiving the response.

        Notes:
            - If `abort_event` is provided and set during the request, the request will be canceled,
              and a `ConnectError` with code `Code.CANCELED` will be raised.
            - The response stream is unmarshaled and validated after the request is completed.

        """
        extensions = {}
        if timeout:
            extensions["timeout"] = {"read": timeout}
            self._request_headers[CONNECT_HEADER_TIMEOUT] = str(int(timeout * 1000))

        content_iterator = self.marshaler.marshal(messages)

        request = httpcore.Request(
            method=HTTPMethod.POST,
            url=httpcore.URL(
                scheme=self.url.scheme,
                host=self.url.host or "",
                port=self.url.port,
                target=self.url.raw_path,
            ),
            headers=list(
                include_request_headers(
                    headers=self._request_headers, url=self.url, content=content_iterator, method=HTTPMethod.POST
                ).items()
            ),
            content=content_iterator,
            extensions=extensions,
        )

        for hook in self._event_hooks["request"]:
            hook(request)

        with map_httpcore_exceptions():
            if not abort_event:
                response = await self.session.pool.handle_async_request(request)
            else:
                request_task = asyncio.create_task(self.session.pool.handle_async_request(request=request))
                abort_task = asyncio.create_task(abort_event.wait())

                done, _ = await asyncio.wait({request_task, abort_task}, return_when=asyncio.FIRST_COMPLETED)

                if abort_task in done:
                    request_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await request_task

                    raise ConnectError("request aborted", Code.CANCELED)

                abort_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await abort_task

                response = await request_task

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = HTTPCoreResponseAsyncByteStream(aiterator=response.stream)

        await self._validate_response(response)

    async def _validate_response(self, response: httpcore.Response) -> None:
        response_headers = Headers(response.headers)

        if response.status != HTTPStatus.OK:
            try:
                await response.aread()
            finally:
                await response.aclose()

            raise ConnectError(
                f"HTTP {response.status}",
                code_from_http_status(response.status),
            )

        response_content_type = response_headers.get(HEADER_CONTENT_TYPE, "")
        if not response_content_type.startswith(CONNECT_STREAMING_CONTENT_TYPE_PREFIX):
            raise ConnectError(
                f"invalid content-type: {response_content_type}; expecting {CONNECT_STREAMING_CONTENT_TYPE_PREFIX}",
                Code.UNKNOWN,
            )

        response_codec_name = connect_codec_from_content_type(self.spec.stream_type, response_content_type)
        if response_codec_name != self.codec.name:
            raise ConnectError(
                f"invalid content-type: {response_content_type}; expecting {CONNECT_STREAMING_CONTENT_TYPE_PREFIX + self.codec.name}",
                Code.INTERNAL,
            )

        compression = response_headers.get(CONNECT_STREAMING_HEADER_COMPRESSION, None)
        if (
            compression
            and compression != COMPRESSION_IDENTITY
            and not any(c.name == compression for c in self.compressions)
        ):
            raise ConnectError(
                f"unknown encoding {compression}: accepted encodings are {', '.join(c.name for c in self.compressions)}",
                Code.INTERNAL,
            )

        self.unmarshaler.compression = get_compresion_from_name(compression, self.compressions)
        self._response_headers.update(response_headers)

    async def aclose(self) -> None:
        """Asynchronously closes the connection by invoking the `aclose` method of the unmarshaler.

        Returns:
            None

        """
        await self.unmarshaler.aclose()
