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

"""Provides the main client implementation for making Connect protocol RPCs."""

import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import httpcore
from yarl import URL

from connect.call_options import CallOptions
from connect.client_interceptor import apply_interceptors
from connect.code import Code
from connect.codec import Codec, CodecNameType, ProtoBinaryCodec, ProtoJSONCodec
from connect.compression import COMPRESSION_IDENTITY, Compression, GZipCompression, get_compression_from_name
from connect.connect import (
    Spec,
    StreamRequest,
    StreamResponse,
    StreamType,
    UnaryRequest,
    UnaryResponse,
    receive_stream_response,
    receive_unary_response,
)
from connect.connection_pool import AsyncConnectionPool
from connect.error import ConnectError
from connect.idempotency_level import IdempotencyLevel
from connect.options import ClientOptions
from connect.protocol import Protocol, ProtocolClient, ProtocolClientParams
from connect.protocol_connect.connect_protocol import ProtocolConnect
from connect.protocol_grpc.grpc_protocol import ProtocolGRPC
from connect.utils import aiterate


def parse_request_url(raw_url: str) -> URL:
    """Parses and validates a request URL.

    Args:
        raw_url: The URL string to parse.

    Returns:
        A validated URL object.

    Raises:
        ConnectError: If the URL is missing a valid scheme (http or https).
    """
    url = URL(raw_url)

    if url.scheme not in ["http", "https"]:
        raise ConnectError(
            f"URL {raw_url} missing scheme: use http:// or https://",
            Code.UNAVAILABLE,
        )

    return url


class ClientConfig:
    """Configuration for a Connect client.

    This class holds all the configuration required to make a request with the client,
    parsing the raw URL and client options into a structured format.

        url (URL): The parsed request URL.
        protocol (Protocol): The protocol implementation to use (e.g., Connect, gRPC, gRPC-Web).
        procedure (str): The full procedure string, including the service and method name (e.g., /acme.user.v1.UserService/GetUser).
        codec (Codec): The codec for marshaling and unmarshaling request/response messages.
        request_compression_name (str | None): The name of the compression algorithm to use for requests.
        compressions (list[Compression]): A list of supported compression implementations.
        descriptor (Any): The protobuf descriptor for the service.
        idempotency_level (IdempotencyLevel): The idempotency level for the procedure.
        compress_min_bytes (int): The minimum message size in bytes to be eligible for compression.
        read_max_bytes (int): The maximum number of bytes to read from a response.
        send_max_bytes (int): The maximum number of bytes to send in a request.
        enable_get (bool): Whether to enable GET requests for idempotent procedures.
    """

    url: URL
    protocol: Protocol
    procedure: str
    codec: Codec
    request_compression_name: str | None
    compressions: list[Compression]
    descriptor: Any
    idempotency_level: IdempotencyLevel
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    enable_get: bool

    def __init__(self, raw_url: str, options: ClientOptions):
        """Initializes a new client instance.

        This method configures the client based on the provided URL and options.
        It sets up the protocol (Connect, gRPC, or gRPC-Web), the message codec
        (Protobuf binary or JSON), compression settings, and other operational
        parameters.

        Args:
            raw_url (str): The full URL for the RPC endpoint.
            options (ClientOptions): An object containing configuration options for the client.

        Raises:
            ConnectError: If an unknown compression algorithm is specified in the options.
        """
        url = parse_request_url(raw_url)
        proto_path = url.path

        self.url = url
        self.protocol = ProtocolConnect()
        if options.protocol == "grpc":
            self.protocol = ProtocolGRPC(web=False)
        elif options.protocol == "grpc-web":
            self.protocol = ProtocolGRPC(web=True)
        self.procedure = proto_path

        if options.use_binary_format:
            self.codec = ProtoBinaryCodec()
        else:
            self.codec = ProtoJSONCodec(CodecNameType.JSON)

        self.request_compression_name = options.request_compression_name
        self.compressions = [GZipCompression()]
        if self.request_compression_name and self.request_compression_name != COMPRESSION_IDENTITY:
            compression = get_compression_from_name(self.request_compression_name, self.compressions)
            if not compression:
                raise ConnectError(
                    f"unknown compression: {self.request_compression_name}",
                    Code.UNKNOWN,
                )
        self.descriptor = options.descriptor
        self.idempotency_level = options.idempotency_level
        self.compress_min_bytes = options.compress_min_bytes
        self.read_max_bytes = options.read_max_bytes
        self.send_max_bytes = options.send_max_bytes
        self.enable_get = options.enable_get

    def spec(self, stream_type: StreamType) -> Spec:
        """Builds a specification for a given stream type.

        This method combines the procedure's general configuration (like procedure name,
        descriptor, and idempotency level) with a specific stream type to create
        a complete `Spec` object.

        Args:
            stream_type: The type of the stream for which to create the spec.

        Returns:
            A `Spec` object tailored to the specified stream type.
        """
        return Spec(
            procedure=self.procedure,
            descriptor=self.descriptor,
            stream_type=stream_type,
            idempotency_level=self.idempotency_level,
        )


class Client[T_Request, T_Response]:
    """A generic client for making Connect protocol RPCs.

    This client is responsible for making unary and streaming RPCs to a Connect-compliant server.
    It is initialized with a connection pool, a server URL, request and response message types,
    and optional configurations. It abstracts the underlying protocol details, allowing users
    to make different types of RPC calls (unary, server-stream, client-stream, bidi-stream)
    through a unified interface.

    Type Parameters:
        T_Request: The type of the request message.
        T_Response: The type of the response message.


    Attributes:
        config (ClientConfig): The configuration used by the client.
        protocol_client (ProtocolClient): The underlying protocol-specific client.
    """

    config: ClientConfig
    protocol_client: ProtocolClient
    _call_unary: Callable[[UnaryRequest[T_Request], CallOptions | None], Awaitable[UnaryResponse[T_Response]]]
    _call_stream: Callable[
        [StreamType, StreamRequest[T_Request], CallOptions | None], Awaitable[StreamResponse[T_Response]]
    ]

    def __init__(
        self,
        pool: AsyncConnectionPool,
        url: str,
        input: type[T_Request],
        output: type[T_Response],
        options: ClientOptions | None = None,
    ) -> None:
        """Initializes a client for a specific RPC method.

        This constructor sets up the necessary components for making RPC calls to a single method.
        It configures the protocol client based on the provided options and prepares wrapped,
        interceptor-aware functions for both unary and streaming calls. These internal call
        functions handle request/response validation, header manipulation, and the actual
        network communication.

        Args:
            pool: The asynchronous connection pool to use for HTTP requests.
            url: The full URL of the RPC method.
            input: The expected type of the request message object.
            output: The expected type of the response message object.
            options: Optional client configuration.
        """
        options = options or ClientOptions()
        config = ClientConfig(url, options)
        self.config = config

        protocol_client = config.protocol.client(
            ProtocolClientParams(
                pool=pool,
                codec=config.codec,
                url=config.url,
                compression_name=config.request_compression_name,
                compressions=config.compressions,
                compress_min_bytes=config.compress_min_bytes,
                read_max_bytes=config.read_max_bytes,
                send_max_bytes=config.send_max_bytes,
                enable_get=config.enable_get,
            )
        )
        self.protocol_client = protocol_client

        unary_spec = config.spec(StreamType.Unary)

        async def _unary_func(request: UnaryRequest[T_Request], call_options: CallOptions) -> UnaryResponse[T_Response]:
            conn = protocol_client.conn(unary_spec, request.headers)

            def on_request_send(r: httpcore.Request) -> None:
                method = r.method
                try:
                    request.method = method.decode("ascii")
                except UnicodeDecodeError as e:
                    raise TypeError(f"method must be ascii encoded: {method!r}") from e

            conn.on_request_send(on_request_send)

            await conn.send(aiterate([request.message]), call_options.timeout, abort_event=call_options.abort_event)

            response = await receive_unary_response(conn=conn, t=output, abort_event=call_options.abort_event)
            return response

        unary_func = apply_interceptors(_unary_func, options.interceptors)

        async def call_unary(
            request: UnaryRequest[T_Request], call_options: CallOptions | None
        ) -> UnaryResponse[T_Response]:
            request.spec = unary_spec
            request.peer = protocol_client.peer
            protocol_client.write_request_headers(StreamType.Unary, request.headers)

            call_options = call_options or CallOptions()

            if not isinstance(request.message, input):
                raise ConnectError(
                    f"expected request of type: {input.__name__}",
                    Code.INTERNAL,
                )

            response = await unary_func(request, call_options)

            if not isinstance(response.message, output):
                raise ConnectError(
                    f"expected response of type: {output.__name__}",
                    Code.INTERNAL,
                )

            return response

        async def _stream_func(
            request: StreamRequest[T_Request], call_options: CallOptions
        ) -> StreamResponse[T_Response]:
            conn = protocol_client.conn(request.spec, request.headers)

            def on_request_send(r: httpcore.Request) -> None:
                method = r.method
                try:
                    request.method = method.decode("ascii")
                except UnicodeDecodeError as e:
                    raise TypeError(f"method must be ascii encoded: {method!r}") from e

            conn.on_request_send(on_request_send)

            await conn.send(request.messages, call_options.timeout, call_options.abort_event, request.matadata)

            response = await receive_stream_response(
                conn, output, request.spec, call_options.abort_event, request.matadata
            )
            return response

        stream_func = apply_interceptors(_stream_func, options.interceptors)

        async def call_stream(
            stream_type: StreamType, request: StreamRequest[T_Request], call_options: CallOptions | None
        ) -> StreamResponse[T_Response]:
            request.spec = config.spec(stream_type)
            request.peer = protocol_client.peer
            protocol_client.write_request_headers(stream_type, request.headers)

            call_options = call_options or CallOptions()

            return await stream_func(request, call_options)

        self._call_unary = call_unary
        self._call_stream = call_stream

    async def call_unary(
        self, request: UnaryRequest[T_Request], call_options: CallOptions | None = None
    ) -> UnaryResponse[T_Response]:
        """Calls a unary RPC method.

        This method sends a single request to the server and receives a single
        response. It is a simple request-response pattern.

        Args:
            request: The unary request object containing the message to be sent.
            call_options: Optional configuration for the call, such as timeouts
                or metadata.

        Returns:
            An awaitable that resolves to the unary response from the server.
        """
        return await self._call_unary(request, call_options)

    @contextlib.asynccontextmanager
    async def call_server_stream(
        self, request: StreamRequest[T_Request], call_options: CallOptions | None = None
    ) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Calls a server-streaming RPC.

        Args:
            request (StreamRequest[T_Request]): The request object for the RPC.
            call_options (CallOptions | None): Optional call options for the RPC.

        Yields:
            AsyncGenerator[StreamResponse[T_Response]]: An asynchronous generator that yields
            the response stream object. The caller is responsible for iterating over this
            object to receive messages from the server. The stream is automatically closed
            when the generator context is exited.
        """
        response = await self._call_stream(StreamType.ServerStream, request, call_options)
        try:
            yield response
        finally:
            await response.aclose()

    @contextlib.asynccontextmanager
    async def call_client_stream(
        self, request: StreamRequest[T_Request], call_options: CallOptions | None = None
    ) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Initiates a client-streaming RPC.

        In a client-streaming RPC, the client sends a sequence of messages to the
        server using a provided stream. Once the client has finished writing the
        messages, it waits for the server to read them and return a single response.

        This method returns an async generator that yields a single `StreamResponse`
        object. The generator pattern is used to ensure that the underlying stream
        is properly closed after use. You should use this method in an `async for`
        loop to correctly manage the stream's lifecycle.

        Args:
            request: The `StreamRequest` object, which includes the RPC method
                and an async iterable of request messages to be sent.
            call_options: Optional configuration for the call, such as timeout
                or metadata.

        Yields:
            A single `StreamResponse` object that can be used to receive the
            server's final response message.
        """
        response = await self._call_stream(StreamType.ClientStream, request, call_options)
        try:
            yield response
        finally:
            await response.aclose()

    @contextlib.asynccontextmanager
    async def call_bidi_stream(
        self, request: StreamRequest[T_Request], call_options: CallOptions | None = None
    ) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Calls a bidirectional streaming method.

        This method initiates a bidirectional streaming call and returns an async generator
        that yields a single `StreamResponse` object. The caller is then responsible for
        iterating over the yielded `StreamResponse` to receive the response messages from
        the server.

        The stream is automatically closed when the context manager exits.

        Args:
            request (StreamRequest[T_Request]): The request object, containing the
                method details and an async iterable of request messages.
            call_options (CallOptions | None): Optional call-specific configurations.

        Yields:
            StreamResponse[T_Response]: An async iterable response object that can be
            iterated over to receive messages from the server.
        """
        response = await self._call_stream(StreamType.BiDiStream, request, call_options)
        try:
            yield response
        finally:
            await response.aclose()
