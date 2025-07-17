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

"""gRPC protocol client implementation for asynchronous Python clients."""

import asyncio
import contextlib
import functools
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from http import HTTPMethod
from typing import Any

import httpcore
from yarl import URL

from connect.code import Code
from connect.codec import Codec
from connect.compression import COMPRESSION_IDENTITY, Compression, get_compression_from_name
from connect.connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamType,
)
from connect.connection_pool import AsyncConnectionPool
from connect.content_stream import BoundAsyncStream
from connect.error import ConnectError
from connect.headers import Headers, include_request_headers
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    ProtocolClient,
    ProtocolClientParams,
    code_from_http_status,
)
from connect.protocol_grpc.constants import (
    DEFAULT_GRPC_USER_AGENT,
    GRPC_HEADER_ACCEPT_COMPRESSION,
    GRPC_HEADER_COMPRESSION,
    GRPC_HEADER_TIMEOUT,
    GRPC_TIMEOUT_MAX_VALUE,
    HEADER_X_USER_AGENT,
    UNIT_TO_SECONDS,
)
from connect.protocol_grpc.content_type import grpc_content_type_from_codec_name, grpc_validate_response_content_type
from connect.protocol_grpc.error_trailer import grpc_error_from_trailer
from connect.protocol_grpc.marshaler import GRPCMarshaler
from connect.protocol_grpc.unmarshaler import GRPCUnmarshaler
from connect.utils import map_httpcore_exceptions

EventHook = Callable[..., Any]


class GRPCClient(ProtocolClient):
    """GRPCClient is a gRPC protocol client implementation that manages connection parameters, peer association, and request header configuration for gRPC or HTTP/2 communication. It supports both standard and web environments, handling codec selection, compression negotiation, and header mutation for outgoing requests.

    Attributes:
        params (ProtocolClientParams): Configuration parameters for the protocol client, including codec, compression, and connection pool.
        _peer (Peer): The peer instance associated with this client, representing the remote endpoint.
        web (bool): Indicates if the client operates in a web environment, affecting header and content-type handling.

    Methods:
        __init__(params: ProtocolClientParams, peer: Peer, web: bool) -> None:
            Initializes the GRPCClient with the provided parameters, peer, and environment flag.

        peer -> Peer:
            Returns the associated Peer object.

        write_request_headers(_: StreamType, headers: Headers) -> None:
            Modifies the provided headers dictionary in place to ensure compliance with gRPC protocol requirements, including user agent, content type, compression, and environment-specific headers.

        conn(spec: Spec, headers: Headers) -> StreamingClientConn:
            Creates and returns a configured GRPCClientConn instance for the specified protocol/service specification and request headers, initializing marshaling and unmarshaling logic with appropriate codecs and compression settings.
    """

    params: ProtocolClientParams
    _peer: Peer
    web: bool

    def __init__(self, params: ProtocolClientParams, peer: Peer, web: bool) -> None:
        """Initializes the gRPC client with the given parameters.

        Args:
            params (ProtocolClientParams): The parameters for the protocol client.
            peer (Peer): The peer instance to connect to.
            web (bool): Indicates whether the client is running in a web environment.
        """
        self.params = params
        self._peer = peer
        self.web = web

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this client.

        Returns:
            Peer: The peer instance representing the remote endpoint.
        """
        return self._peer

    def write_request_headers(self, _: StreamType, headers: Headers) -> None:
        """Sets and modifies HTTP/2 or gRPC-specific headers for an outgoing request.

        This method ensures that required headers such as `User-Agent`, `Content-Type`, and compression-related headers
        are present and correctly set based on the client's configuration and the request context.

        Args:
            _ (StreamType): The stream associated with the request (unused in this method).
            headers (Headers): The dictionary of HTTP headers to be modified in-place.

        Behavior:
            - Sets a default `User-Agent` header if not already present.
            - For web clients, sets an additional `X-User-Agent` header if not present.
            - Sets the `Content-Type` header based on the codec and web mode.
            - Sets the `Accept-Encoding` header to indicate supported compression.
            - If a specific compression is configured, sets the corresponding gRPC compression header.
            - If multiple compressions are supported, sets the `Accept-Encoding` header accordingly.
            - For non-web clients, adds the `Te: trailers` header as required by gRPC.

        Returns:
            None
        """
        if headers.get(HEADER_USER_AGENT, None) is None:
            headers[HEADER_USER_AGENT] = DEFAULT_GRPC_USER_AGENT

        if self.web and headers.get(HEADER_X_USER_AGENT, None) is None:
            headers[HEADER_X_USER_AGENT] = DEFAULT_GRPC_USER_AGENT

        headers[HEADER_CONTENT_TYPE] = grpc_content_type_from_codec_name(self.web, self.params.codec.name)

        headers["Accept-Encoding"] = COMPRESSION_IDENTITY
        if self.params.compression_name and self.params.compression_name != COMPRESSION_IDENTITY:
            headers[GRPC_HEADER_COMPRESSION] = self.params.compression_name

        if self.params.compressions:
            headers[GRPC_HEADER_ACCEPT_COMPRESSION] = ", ".join(c.name for c in self.params.compressions)

        if not self.web:
            headers["Te"] = "trailers"

    def conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Creates and returns a GRPCClientConn instance configured with the provided specification and headers.

        Args:
            spec (Spec): The specification object defining the gRPC method and message types.
            headers (Headers): The request headers to include in the gRPC call.

        Returns:
            StreamingClientConn: An instance of GRPCClientConn configured for streaming communication.

        """
        return GRPCClientConn(
            web=self.web,
            pool=self.params.pool,
            spec=spec,
            peer=self.peer,
            url=self.params.url,
            codec=self.params.codec,
            compressions=self.params.compressions,
            marshaler=GRPCMarshaler(
                codec=self.params.codec,
                compress_min_bytes=self.params.compress_min_bytes,
                send_max_bytes=self.params.send_max_bytes,
                compression=get_compression_from_name(self.params.compression_name, self.params.compressions),
            ),
            unmarshaler=GRPCUnmarshaler(
                web=self.web,
                codec=self.params.codec,
                read_max_bytes=self.params.read_max_bytes,
            ),
            request_headers=headers,
        )


class GRPCClientConn(StreamingClientConn):
    """GRPCClientConn manages a gRPC client connection over HTTP/2, supporting streaming, compression, and custom codecs.

    This class is responsible for sending and receiving gRPC messages asynchronously, handling request/response headers and trailers, managing connection pooling, and supporting event hooks for request and response lifecycle events. It abstracts the details of HTTP/2 transport and gRPC protocol compliance, including marshaling/unmarshaling messages, applying compression, and error handling.

    Attributes:
        web (bool): Indicates if the connection is for a web environment.
        pool (AsyncConnectionPool): The connection pool for managing HTTP/2 connections.
        _spec (Spec): The specification object describing the protocol or API.
        _peer (Peer): The peer information for the connection.
        url (URL): The URL endpoint for the connection.
        codec (Codec | None): The codec to use for encoding/decoding messages, or None.
        compressions (list[Compression]): List of supported compression algorithms.
        marshaler (GRPCMarshaler): The marshaler for serializing messages.
        unmarshaler (GRPCUnmarshaler): The unmarshaler for deserializing messages.
        _response_headers (Headers): Stores response headers.
        _response_trailers (Headers): Stores response trailers.
        _request_headers (Headers): Stores request headers.
        receive_trailers (Callable[[], None] | None): Callback to receive trailers after response.
    """

    web: bool
    pool: AsyncConnectionPool
    _spec: Spec
    _peer: Peer
    url: URL
    codec: Codec | None
    compressions: list[Compression]
    marshaler: GRPCMarshaler
    unmarshaler: GRPCUnmarshaler
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers
    receive_trailers: Callable[[], None] | None

    def __init__(
        self,
        web: bool,
        pool: AsyncConnectionPool,
        spec: Spec,
        peer: Peer,
        url: URL,
        codec: Codec | None,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: GRPCMarshaler,
        unmarshaler: GRPCUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initializes a new instance of the gRPC client.

        Args:
            web (bool): Indicates if the client is running in a web environment.
            pool (AsyncConnectionPool): The asynchronous connection pool to use for connections.
            spec (Spec): The service specification.
            peer (Peer): The peer information for the connection.
            url (URL): The URL of the gRPC server.
            codec (Codec | None): The codec to use for message serialization, or None.
            compressions (list[Compression]): List of supported compression algorithms.
            request_headers (Headers): Headers to include in each request.
            marshaler (GRPCMarshaler): The marshaler for serializing requests.
            unmarshaler (GRPCUnmarshaler): The unmarshaler for deserializing responses.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Optional mapping of event hooks for "request" and "response" events.
        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.web = web
        self.pool = pool
        self._spec = spec
        self._peer = peer
        self.url = url
        self.codec = codec
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
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
        """Returns the current Peer instance associated with this client.

        Returns:
            Peer: The peer instance representing the remote endpoint for this client.
        """
        return self._peer

    async def receive(self, message: Any, abort_event: asyncio.Event | None) -> AsyncIterator[Any]:
        """Receives a message and processes it."""
        trailer_received = False

        async for obj, end in self.unmarshaler.unmarshal(message):
            if abort_event and abort_event.is_set():
                raise ConnectError("receive operation aborted", Code.CANCELED)

            if end:
                if trailer_received:
                    raise ConnectError("received extra end stream trailer", Code.INVALID_ARGUMENT)

                trailer_received = True
                if self.unmarshaler.web_trailers is None:
                    raise ConnectError("trailer not received", Code.INVALID_ARGUMENT)

                continue

            if trailer_received:
                raise ConnectError("protocol error: received extra message after trailer", Code.INVALID_ARGUMENT)

            yield obj

        if callable(self.receive_trailers):
            self.receive_trailers()

        if self.unmarshaler.bytes_read == 0 and len(self.response_trailers) == 0:
            self.response_trailers.update(self._response_headers)
            if HEADER_CONTENT_TYPE in self._response_headers:
                del self._response_headers[HEADER_CONTENT_TYPE]

            server_error = grpc_error_from_trailer(self.response_trailers)
            if server_error:
                server_error.metadata = self.response_headers.copy()
                raise server_error

        server_error = grpc_error_from_trailer(self.response_trailers)
        if server_error:
            server_error.metadata = self.response_headers.copy()
            server_error.metadata.update(self.response_trailers.copy())
            raise server_error

    def _receive_trailers(self, response: httpcore.Response) -> None:
        if self.web:
            trailers = self.unmarshaler.web_trailers
            if trailers is not None:
                self._response_trailers.update(trailers)

        else:
            if "trailing_headers" not in response.extensions:
                return

            trailers = response.extensions["trailing_headers"]
            self._response_trailers.update(Headers(trailers))

    @property
    def request_headers(self) -> Headers:
        """Returns the headers to be included in the gRPC request.

        Returns:
            Headers: The headers used for the gRPC request.
        """
        return self._request_headers

    async def send(
        self, messages: AsyncIterable[Any], timeout: float | None, abort_event: asyncio.Event | None
    ) -> None:
        """Sends a gRPC request asynchronously using HTTP/2 via httpcore.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled and sent as the request body.
            timeout (float | None): Optional timeout in seconds for the request. If provided, sets the gRPC timeout header.
            abort_event (asyncio.Event | None): Optional asyncio event that, when set, aborts the request.

        Raises:
            ConnectError: If the request is aborted via the abort_event.
            Exception: Propagates exceptions raised by the underlying HTTP client or marshaling/unmarshaling logic.

        Side Effects:
            - Invokes registered request and response event hooks.
            - Sets up the response stream for unmarshaling.
            - Validates the response after receiving it.

        """
        extensions = {}
        if timeout:
            extensions["timeout"] = {"read": timeout}
            self._request_headers[GRPC_HEADER_TIMEOUT] = grpc_encode_timeout(timeout)

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
        self.receive_trailers = functools.partial(self._receive_trailers, response)

        await self._validate_response(response)

    async def _validate_response(self, response: httpcore.Response) -> None:
        response_headers = Headers(response.headers)
        if response.status != 200:
            raise ConnectError(
                f"HTTP {response.status}",
                code_from_http_status(response.status),
            )

        grpc_validate_response_content_type(
            self.web,
            self.marshaler.codec.name if self.marshaler.codec else "",
            response_headers.get(HEADER_CONTENT_TYPE, ""),
        )

        compression = response_headers.get(GRPC_HEADER_COMPRESSION, None)
        if compression and compression != COMPRESSION_IDENTITY:
            self.unmarshaler.compression = get_compression_from_name(compression, self.compressions)

        self._response_headers.update(response_headers)

    @property
    def response_headers(self) -> Headers:
        """Returns the headers received in the response.

        Returns:
            Headers: The response headers.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers as headers.

        Returns:
            Headers: The response trailers received from the gRPC call.
        """
        return self._response_trailers

    def on_request_send(self, fn: EventHook) -> None:
        """Registers a callback function to be invoked when a request is sent.

        Args:
            fn (EventHook): The callback function to be added to the "request" event hook.

        Returns:
            None
        """
        self._event_hooks["request"].append(fn)

    async def aclose(self) -> None:
        """Asynchronously closes the resources associated with the client.

        This method ensures that any resources held by the `unmarshaler` are properly released.
        It should be called when the client is no longer needed to avoid resource leaks.
        """
        await self.unmarshaler.aclose()


def grpc_encode_timeout(timeout: float) -> str:
    """Encodes a timeout value (in seconds) into a gRPC-compatible timeout string.

    The gRPC protocol requires timeout values to be specified as a string with a numeric value and a unit suffix.
    This function converts a floating-point timeout (in seconds) into the appropriate string format, choosing the
    largest unit possible without exceeding the maximum value allowed for that unit.

    Args:
        timeout (float): The timeout value in seconds.

    Returns:
        str: The timeout encoded as a gRPC timeout string (e.g., "10S", "500m", "0n").

    Notes:
        - If the timeout is less than or equal to zero, "0n" is returned.
        - The function uses predefined unit-to-seconds mappings and a maximum value per unit.
        - If the timeout exceeds all unit ranges, it is encoded in hours ("H").
    """
    if timeout <= 0:
        return "0n"

    grpc_timeout_max_value = GRPC_TIMEOUT_MAX_VALUE

    _units = dict(sorted(UNIT_TO_SECONDS.items(), key=lambda item: item[1]))
    for unit, size in _units.items():
        if timeout < size * grpc_timeout_max_value:
            value = int(timeout / size)
            return f"{value}{unit}"

    value = int(timeout / 3600.0)
    return f"{value}H"
