# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Connect protocol client implementation for unary and streaming RPCs."""

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

from connect.code import Code
from connect.codec import Codec, StableCodec
from connect.compression import COMPRESSION_IDENTITY, Compression, get_compression_from_name
from connect.connect import (
    Address,
    Peer,
    Spec,
    StreamingClientConn,
    StreamType,
    ensure_single,
)
from connect.connection_pool import AsyncConnectionPool
from connect.content_stream import BoundAsyncStream
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
from connect.utils import (
    map_httpcore_exceptions,
)

EventHook = Callable[..., Any]


class ConnectClient(ProtocolClient):
    """ConnectClient is a client implementation for the Connect protocol, extending ProtocolClient.

    This class is responsible for initializing and managing the connection parameters, peer information,
    and handling the construction of request headers and client connections for both unary and streaming
    communication patterns.

    Attributes:
        params (ProtocolClientParams): The parameters required to initialize the client, including codec,
            compression settings, connection pool, and URL.
        _peer (Peer): The peer instance representing the remote endpoint for the connection.
    """

    params: ProtocolClientParams
    _peer: Peer

    def __init__(self, params: ProtocolClientParams) -> None:
        """Initializes the ConnectClient with the given protocol client parameters.

        Args:
            params (ProtocolClientParams): The parameters required to configure the protocol client, including URL information.

        Attributes:
            params (ProtocolClientParams): Stores the provided protocol client parameters.
            _peer (Peer): Represents the peer connection, initialized with the host and port from the provided URL, using the CONNECT protocol.
        """
        self.params = params
        self._peer = Peer(
            address=Address(host=params.url.host or "", port=params.url.port or 80),
            protocol=PROTOCOL_CONNECT,
            query={},
        )

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this client.

        Returns:
            Peer: The peer instance associated with this client.
        """
        return self._peer

    def write_request_headers(self, stream_type: StreamType, headers: Headers) -> None:
        """Sets and updates HTTP headers for a Connect protocol request based on the stream type and client parameters.

        Args:
            stream_type (StreamType): The type of stream (e.g., Unary or Streaming) for the request.
            headers (Headers): The dictionary of HTTP headers to be sent with the request. This dictionary is modified in-place.

        Behavior:
            - Ensures the 'User-Agent' header is set to a default value if not already present.
            - Sets the Connect protocol version and appropriate 'Content-Type' header based on the codec.
            - Configures compression-related headers depending on the stream type and client compression settings.
            - For streaming requests, sets both accepted and used compression headers if applicable.
            - Updates the 'Accept-Encoding' header to list all supported compression algorithms if provided.

        Modifies:
            The `headers` dictionary is updated in-place with the necessary protocol and compression headers.
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
        """Creates and returns a streaming client connection based on the provided specification and headers.

        Depending on the `stream_type` in the `spec`, this method initializes either a unary or streaming client connection
        with the appropriate marshaler and unmarshaler configurations.

        Args:
            spec (Spec): The specification for the connection, including stream type and idempotency level.
            headers (Headers): The request headers to be used for the connection.

        Returns:
            StreamingClientConn: An initialized client connection object, either unary or streaming.

        Notes:
            - For unary connections with `IdempotencyLevel.NO_SIDE_EFFECTS`, additional marshaler parameters are set.
            - The connection is configured with compression, codec, and byte limit settings as specified in `self.params`.
        """
        conn: StreamingClientConn
        if spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryClientConn(
                pool=self.params.pool,
                spec=spec,
                peer=self.peer,
                url=self.params.url,
                compressions=self.params.compressions,
                request_headers=headers,
                marshaler=ConnectUnaryRequestMarshaler(
                    codec=self.params.codec,
                    compression=get_compression_from_name(self.params.compression_name, self.params.compressions),
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
                pool=self.params.pool,
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
                    compression=get_compression_from_name(self.params.compression_name, self.params.compressions),
                ),
                unmarshaler=ConnectStreamingUnmarshaler(
                    codec=self.params.codec,
                    read_max_bytes=self.params.read_max_bytes,
                ),
            )

        return conn


class ConnectUnaryClientConn(StreamingClientConn):
    """ConnectUnaryClientConn provides a client-side connection for unary (single-request, single-response) Connect protocol calls.

    This class manages the lifecycle of a unary request, including marshaling the request, sending it over HTTP (GET or POST),
    handling timeouts and aborts, processing response headers and trailers, and unmarshaling the response. It also supports
    event hooks for request and response processing, and manages compression and content-type validation.

    Attributes:
        pool (AsyncConnectionPool): The connection pool used for sending requests.
        _spec (Spec): The protocol specification for the connection.
        _peer (Peer): The peer information for the connection.
        url (URL): The target URL for the connection.
        compressions (list[Compression]): Supported compression methods.
        marshaler (ConnectUnaryRequestMarshaler): Marshaler for encoding requests.
        unmarshaler (ConnectUnaryUnmarshaler): Unmarshaler for decoding responses.
        response_content (bytes | None): The raw response content, if available.
        _response_headers (Headers): Headers received in the response.
        _response_trailers (Headers): Trailers received after the response body.
        _request_headers (Headers): Headers to send with the request.
        _event_hooks (dict[str, list[EventHook]]): Registered event hooks for request and response events.
    """

    pool: AsyncConnectionPool
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
    _event_hooks: dict[str, list[EventHook]]

    def __init__(
        self,
        pool: AsyncConnectionPool,
        spec: Spec,
        peer: Peer,
        url: URL,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: ConnectUnaryRequestMarshaler,
        unmarshaler: ConnectUnaryUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initializes a new instance of the client.

        Args:
            pool (AsyncConnectionPool): The connection pool to use for managing connections.
            spec (Spec): The specification object describing the protocol or service.
            peer (Peer): The peer information for the connection.
            url (URL): The URL endpoint for the connection.
            compressions (list[Compression]): List of supported compression algorithms.
            request_headers (Headers): Headers to include in outgoing requests.
            marshaler (ConnectUnaryRequestMarshaler): Marshaler for serializing requests.
            unmarshaler (ConnectUnaryUnmarshaler): Unmarshaler for deserializing responses.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Optional mapping of event hooks for "request" and "response" events. Defaults to None.

        Attributes:
            pool (AsyncConnectionPool): The connection pool instance.
            _spec (Spec): The protocol or service specification.
            _peer (Peer): The peer information.
            url (URL): The endpoint URL.
            compressions (list[Compression]): Supported compression algorithms.
            marshaler (ConnectUnaryRequestMarshaler): Request marshaler.
            unmarshaler (ConnectUnaryUnmarshaler): Response unmarshaler.
            response_content: The content of the response (initialized as None).
            _response_headers (Headers): Headers from the response.
            _response_trailers (Headers): Trailers from the response.
            _request_headers (Headers): Headers for outgoing requests.
            _event_hooks (dict): Event hooks for "request" and "response" events.
        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.pool = pool
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
        """Returns the specification object associated with this client.

        Returns:
            Spec: The specification instance used by the client.
        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this client.

        Returns:
            Peer: The peer instance associated with this client, representing the remote endpoint.
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

    def receive(
        self,
        message: Any,
        _abort_event: asyncio.Event | None,
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        """Receives messages asynchronously based on the provided input message.

        Args:
            message (Any): The input message or request to process.
            _abort_event (asyncio.Event | None): Optional event to signal abortion of the receive operation.

        Yields:
            Any: Messages received from the underlying message stream.

        Returns:
            AsyncIterator[Any]: An asynchronous iterator yielding received messages.
        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Returns the HTTP headers to be included in the request.

        Returns:
            Headers: The headers to be sent with the request.
        """
        return self._request_headers

    def on_request_send(self, fn: EventHook) -> None:
        """Registers a callback function to be invoked whenever a request is sent.

        Args:
            fn (EventHook): The callback function to be added to the 'request' event hook.

        Returns:
            None
        """
        self._event_hooks["request"].append(fn)

    async def send(
        self,
        messages: AsyncIterable[Any],
        timeout: float | None,
        abort_event: asyncio.Event | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Sends a single message asynchronously using either HTTP GET or POST, with optional timeout and abort support.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable yielding the message(s) to send. Only a single message is allowed.
            timeout (float | None): Optional timeout in seconds for the request. If provided, sets the request timeout.
            abort_event (asyncio.Event | None): Optional asyncio event that, when set, aborts the request.

        Raises:
            ConnectError: If the marshaler URL is not set when required, if the request is aborted, or for internal errors.
            Exception: Propagates exceptions raised by the underlying HTTP client.

        Side Effects:
            - Modifies request headers based on timeout and content length.
            - Invokes registered request and response event hooks.
            - Sets the unmarshaler's stream to the response stream.
            - Validates the response.

        Returns:
            None
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
                response = await self.pool.handle_async_request(request=request)
            else:
                request_task = asyncio.create_task(self.pool.handle_async_request(request=request))
                abort_task = asyncio.create_task(abort_event.wait())

                try:
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
                finally:
                    for task in [request_task, abort_task]:
                        if not task.done():
                            task.cancel()

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = BoundAsyncStream(response.stream)

        await self._validate_response(response)

    @property
    def response_headers(self) -> Headers:
        """Returns the headers from the HTTP response.

        Returns:
            Headers: The headers of the HTTP response.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers as a Headers object.

        Response trailers are HTTP headers sent after the response body, typically used in protocols like gRPC.

        Returns:
            Headers: The response trailers associated with the response.
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

        self.unmarshaler.compression = get_compression_from_name(compression, self.compressions)

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
        """Returns the dictionary of registered event hooks.

        Returns:
            dict[str, list[EventHook]]: A dictionary where each key is a string representing the event name,
            and the value is a list of EventHook instances associated with that event.
        """
        return self._event_hooks

    @event_hooks.setter
    def event_hooks(self, event_hooks: dict[str, list[EventHook]]) -> None:
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    async def aclose(self) -> None:
        """Asynchronously closes the client connection and releases any associated resources.

        This method should be called when the client is no longer needed to ensure proper cleanup.
        Currently, this implementation does not perform any actions, but it can be extended in the future.
        """
        return


class ConnectStreamingClientConn(StreamingClientConn):
    """ConnectStreamingClientConn manages a streaming client connection for the Connect protocol.

    This class handles the lifecycle of a streaming RPC client connection, including sending and receiving messages,
    managing request and response headers, handling compression, marshaling/unmarshaling of messages, and supporting
    event hooks for request and response events. It integrates with an asynchronous connection pool and supports
    abortable operations via asyncio events.

    Attributes:
        _spec (Spec): The protocol specification for the connection.
        _peer (Peer): The peer associated with this connection.
        url (URL): The URL endpoint for the connection.
        codec (Codec): Codec used for encoding and decoding messages.
        compressions (list[Compression]): Supported compression methods.
        marshaler (ConnectStreamingMarshaler): Marshaler for outgoing streaming messages.
        unmarshaler (ConnectStreamingUnmarshaler): Unmarshaler for incoming streaming messages.
        response_content (bytes | None): Raw response content, if any.
        _response_headers (Headers): Headers received in the response.
        _response_trailers (Headers): Trailers received after the response body.
        _request_headers (Headers): Headers sent with the request.
    """

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
        pool: AsyncConnectionPool,
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
        """Initializes a new instance of the client.

        Args:
            pool (AsyncConnectionPool): The asynchronous connection pool to use for network operations.
            spec (Spec): The service specification or schema.
            peer (Peer): The peer information for the connection.
            url (URL): The URL endpoint for the connection.
            codec (Codec): The codec used for encoding and decoding messages.
            compressions (list[Compression]): List of supported compression algorithms.
            request_headers (Headers): Headers to include in outgoing requests.
            marshaler (ConnectStreamingMarshaler): Marshaler for streaming request bodies.
            unmarshaler (ConnectStreamingUnmarshaler): Unmarshaler for streaming response bodies.
            event_hooks (Optional[Mapping[str, list[EventHook]]]): Optional mapping of event hooks for 'request' and 'response' events.
        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.pool = pool
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
        """Returns the specification object associated with this client.

        Returns:
            Spec: The specification instance used by the client.
        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this client.

        Returns:
            Peer: The peer instance linked to this client.
        """
        return self._peer

    @property
    def request_headers(self) -> Headers:
        """Returns the HTTP headers to be included in the request.

        Returns:
            Headers: The headers to be sent with the request.
        """
        return self._request_headers

    @property
    def response_headers(self) -> Headers:
        """Returns the HTTP response headers.

        Returns:
            Headers: The headers received in the HTTP response.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers as a Headers object.

        Response trailers are additional HTTP headers sent after the response body,
        typically used in protocols like gRPC for sending metadata at the end of a response.

        Returns:
            Headers: The response trailers associated with the response.
        """
        return self._response_trailers

    def on_request_send(self, fn: EventHook) -> None:
        """Registers a callback function to be invoked whenever a request is sent.

        Args:
            fn (EventHook): The callback function to be executed on request send events.

        Returns:
            None
        """
        self._event_hooks["request"].append(fn)

    async def receive(
        self,
        message: Any,
        abort_event: asyncio.Event | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        """Asynchronously receives and yields messages from the unmarshaler, handling stream control and errors.

        Args:
            message (Any): The incoming message or stream to be unmarshaled.
            abort_event (asyncio.Event | None, optional): An event to signal abortion of the receive operation.
                If set, the operation is canceled and a ConnectError is raised.

        Yields:
            Any: The next unmarshaled object from the message stream.

        Raises:
            ConnectError: If the receive operation is aborted, if extra end stream messages are received,
                if a message is received after the end of the stream, if an error is encountered in the end stream,
                or if the end stream message is missing.
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
        self,
        messages: AsyncIterable[Any],
        timeout: float | None,
        abort_event: asyncio.Event | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Sends a stream of messages asynchronously to the server using HTTP POST.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be sent.
            timeout (float | None): Optional timeout in seconds for the request. If provided, sets the request timeout.
            abort_event (asyncio.Event | None): Optional asyncio event to abort the request. If set and triggered, the request will be cancelled.

        Raises:
            ConnectError: If the request is aborted via the abort_event.
            Exception: Propagates exceptions raised during the request or response handling.

        Side Effects:
            - Invokes registered request and response event hooks.
            - Sets up the response stream for unmarshaling.
            - Validates the server response.

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
                response = await self.pool.handle_async_request(request)
            else:
                request_task = asyncio.create_task(self.pool.handle_async_request(request=request))
                abort_task = asyncio.create_task(abort_event.wait())

                try:
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
                finally:
                    for task in [request_task, abort_task]:
                        if not task.done():
                            task.cancel()

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = BoundAsyncStream(response.stream)

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

        self.unmarshaler.compression = get_compression_from_name(compression, self.compressions)
        self._response_headers.update(response_headers)

    async def aclose(self) -> None:
        """Asynchronously closes the client by closing the associated unmarshaler.

        This method should be called to properly release any resources held by the unmarshaler
        when the client is no longer needed.
        """
        await self.unmarshaler.aclose()
