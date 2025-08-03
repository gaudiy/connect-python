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

"""Defines the server-side handlers for Connect, gRPC, and gRPC-Web RPCs."""

import asyncio
from collections.abc import Awaitable, Callable
from http import HTTPMethod, HTTPStatus
from typing import Any

import anyio
from starlette.responses import PlainTextResponse, Response

from connectrpc.code import Code
from connectrpc.codec import Codec, CodecMap, CodecNameType, ProtoBinaryCodec, ProtoJSONCodec
from connectrpc.compression import Compression, GZipCompression
from connectrpc.connect import (
    Spec,
    StreamingHandlerConn,
    StreamRequest,
    StreamResponse,
    StreamType,
    UnaryRequest,
    UnaryResponse,
    receive_stream_request,
    receive_unary_request,
)
from connectrpc.error import ConnectError
from connectrpc.handler_context import HandlerContext
from connectrpc.handler_interceptor import apply_interceptors
from connectrpc.headers import Headers
from connectrpc.idempotency_level import IdempotencyLevel
from connectrpc.options import HandlerOptions
from connectrpc.protocol import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    ProtocolHandler,
    ProtocolHandlerParams,
    exclude_protocol_headers,
    mapped_method_handlers,
    sorted_accept_post_value,
    sorted_allow_method_value,
)
from connectrpc.protocol_connect.connect_protocol import (
    ProtocolConnect,
)
from connectrpc.protocol_grpc.grpc_protocol import ProtocolGRPC
from connectrpc.request import Request
from connectrpc.response_writer import ServerResponseWriter
from connectrpc.utils import aiterate

type UnaryFunc[T_Request, T_Response] = Callable[
    [UnaryRequest[T_Request], HandlerContext], Awaitable[UnaryResponse[T_Response]]
]
type StreamFunc[T_Request, T_Response] = Callable[
    [StreamRequest[T_Request], HandlerContext], Awaitable[StreamResponse[T_Response]]
]


class HandlerConfig:
    """Configuration for an RPC handler.

    This class encapsulates all the configuration required to execute a specific RPC
    procedure. It includes details about the procedure itself, serialization codecs,
    compression algorithms, and various protocol-level settings.

    Attributes:
        procedure (str): The full name of the RPC procedure (e.g., /acme.foo.v1.FooService/Bar).
        stream_type (StreamType): The type of stream for the procedure (unary, client, server, or bidi).
        codecs (dict[str, Codec]): A dictionary mapping codec names to their respective Codec implementations.
        compressions (list[Compression]): A list of supported compression algorithms.
        descriptor (Any): The protobuf message or service descriptor.
        compress_min_bytes (int): The minimum number of bytes a message must have to be considered for compression.
        read_max_bytes (int): The maximum number of bytes to read for a single message.
        send_max_bytes (int): The maximum number of bytes to send for a single message.
        require_connect_protocol_header (bool): Whether to require the `Connect-Protocol-Version` header.
        idempotency_level (IdempotencyLevel): The idempotency level of the procedure.
    """

    procedure: str
    stream_type: StreamType
    codecs: dict[str, Codec]
    compressions: list[Compression]
    descriptor: Any
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    require_connect_protocol_header: bool
    idempotency_level: IdempotencyLevel

    def __init__(self, procedure: str, stream_type: StreamType, options: HandlerOptions):
        """Initializes a new Handler.

        Args:
            procedure (str): The full name of the RPC procedure.
            stream_type (StreamType): The type of stream for the procedure.
            options (HandlerOptions): Configuration options for the handler.

        Attributes:
            procedure (str): The full name of the RPC procedure.
            stream_type (StreamType): The type of stream for the procedure.
            codecs (dict[str, Codec]): A dictionary of supported codecs, keyed by name.
            compressions (list[Compression]): A list of supported compression algorithms.
            descriptor: The protobuf method descriptor.
            compress_min_bytes (int): The minimum number of bytes for a response to be compressed.
            read_max_bytes (int): The maximum number of bytes to read for a request message.
            send_max_bytes (int): The maximum number of bytes to send for a response message.
            require_connect_protocol_header (bool): Whether to require the Connect protocol header.
            idempotency_level: The idempotency level of the procedure.
        """
        self.procedure = procedure
        self.stream_type = stream_type
        codecs: list[Codec] = [
            ProtoBinaryCodec(),
            ProtoJSONCodec(CodecNameType.JSON),
            ProtoJSONCodec(CodecNameType.JSON_CHARSET_UTF8),
        ]
        self.codecs = {codec.name: codec for codec in codecs}
        self.compressions = [GZipCompression()]
        self.descriptor = options.descriptor
        self.compress_min_bytes = options.compress_min_bytes
        self.read_max_bytes = options.read_max_bytes
        self.send_max_bytes = options.send_max_bytes
        self.require_connect_protocol_header = options.require_connect_protocol_header
        self.idempotency_level = options.idempotency_level

    def spec(self) -> Spec:
        """Get the specification for the handler.

        Returns:
            Spec: A `Spec` object containing the handler's specification,
                including procedure, descriptor, stream type, and idempotency level.
        """
        return Spec(
            procedure=self.procedure,
            descriptor=self.descriptor,
            stream_type=self.stream_type,
            idempotency_level=self.idempotency_level,
        )


def create_protocol_handlers(config: HandlerConfig) -> list[ProtocolHandler]:
    """Creates and configures protocol handlers based on the provided configuration.

    This function initializes handlers for the Connect, gRPC, and gRPC-Web protocols.
    Each handler is configured with parameters extracted from the `config` object,
    such as codecs, compression algorithms, message size limits, and other
    protocol-specific settings.

    Args:
        config: A HandlerConfig object containing the configuration
            for the protocol handlers.

    Returns:
        A list of initialized ProtocolHandler instances.
    """
    protocols = [ProtocolConnect(), ProtocolGRPC(web=False), ProtocolGRPC(web=True)]

    codecs = CodecMap(config.codecs)

    handlers: list[ProtocolHandler] = []
    for protocol in protocols:
        handlers.append(
            protocol.handler(
                params=ProtocolHandlerParams(
                    spec=config.spec(),
                    codecs=codecs,
                    compressions=config.compressions,
                    compress_min_bytes=config.compress_min_bytes,
                    read_max_bytes=config.read_max_bytes,
                    send_max_bytes=config.send_max_bytes,
                    require_connect_protocol_header=config.require_connect_protocol_header,
                    idempotency_level=config.idempotency_level,
                )
            )
        )

    return handlers


class Handler:
    """A base handler for a single RPC procedure.

    This class is responsible for routing an incoming HTTP request to the correct
    protocol-specific handler (e.g., Connect, gRPC, gRPC-Web) based on the
    HTTP method and the Content-Type header. It manages the request lifecycle,
    including validation, asynchronous processing, and error handling.

    Subclasses must implement the `implementation` method to define the
    procedure's business logic.

    Attributes:
        procedure (str): The fully-qualified name of the procedure (e.g., /acme.foo.v1.FooService/Bar).
        protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A mapping of HTTP methods to the protocol handlers that support them.
        allow_methods (str): A comma-separated string of allowed HTTP methods, used in the `Allow` header for 405 responses.
        accept_post (str): A comma-separated string of supported `Content-Type` values for POST requests, used in the `Accept-Post` header for 415 responses.
        protocol_handler (ProtocolHandler): The specific protocol handler chosen to handle the current request. This is set within the `handle` method.
    """

    procedure: str
    protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]]
    allow_methods: str
    accept_post: str
    protocol_handler: ProtocolHandler

    def __init__(
        self,
        procedure: str,
        protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]],
        allow_methods: str,
        accept_post: str,
    ) -> None:
        """Initializes a handler for a specific RPC procedure.

        Args:
            procedure: The full name of the procedure.
            protocol_handlers: A dictionary mapping HTTP methods to a list of
                protocol-specific handlers that can process requests for this procedure.
            allow_methods: The value for the 'Allow' HTTP header, listing supported methods.
            accept_post: The value for the 'Accept-Post' HTTP header, listing supported
                content types for POST requests.
        """
        self.procedure = procedure
        self.protocol_handlers = protocol_handlers
        self.allow_methods = allow_methods
        self.accept_post = accept_post

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """The actual implementation of the streaming handler logic.

        This method must be overridden by subclasses to define the specific
        behavior of the handler. It is called to process the streaming
        connection.

        Args:
            conn (StreamingHandlerConn): The connection object for the streaming
                session, used to send and receive messages.
            timeout (float | None): An optional timeout in seconds for the
                entire handling operation.
        """
        raise NotImplementedError()

    async def handle(self, request: Request) -> Response:
        """Handles an incoming HTTP request and routes it to the appropriate Connect protocol handler.

        This method acts as the main entry point for the Connect service. It performs the
        following steps:
        1.  Validates the HTTP method. If the method is not supported, it returns a
            405 Method Not Allowed response.
        2.  Determines the correct protocol handler (e.g., Connect, gRPC-Web) based on
            the request's Content-Type header. If no suitable handler is found, it
            returns a 415 Unsupported Media Type response.
        3.  For GET requests, it ensures there is no request body, returning a 415
            response if a body is present, as per the Connect protocol specification.
        4.  It creates two concurrent tasks:
            - One to execute the actual RPC logic (`_handle`).
            - One to wait for the response headers to be written by the logic task.
        5.  It waits for the first task to complete. The response is typically generated
            as soon as the headers are available, allowing for streaming responses.
        6.  Ensures proper cleanup by cancelling any lingering tasks.
        7.  Returns the generated `Response` object to the web server.

        Args:
            request: The incoming Starlette Request object.

        Returns:
            A Starlette Response object to be sent to the client.
        """
        response_headers = Headers(encoding="latin-1")
        response_trailers = Headers(encoding="latin-1")

        protocol_handlers = self.protocol_handlers.get(HTTPMethod(request.method))
        if not protocol_handlers:
            response_headers["Allow"] = self.allow_methods
            status = HTTPStatus.METHOD_NOT_ALLOWED
            return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        content_type = request.headers.get(HEADER_CONTENT_TYPE, "")

        protocol_handler: ProtocolHandler | None = None

        for handler in protocol_handlers:
            if handler.can_handle_payload(request, content_type):
                protocol_handler = handler
                break

        if not protocol_handler:
            response_headers["Accept-Post"] = self.accept_post
            status = HTTPStatus.UNSUPPORTED_MEDIA_TYPE
            return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        self.protocol_handler = protocol_handler

        if HTTPMethod(request.method) == HTTPMethod.GET:
            content_length = request.headers.get(HEADER_CONTENT_LENGTH, None)
            has_body = False

            if content_length and int(content_length) > 0:
                has_body = True
            else:
                async for chunk in request.stream():
                    if chunk:
                        has_body = True
                    break

            if has_body:
                status = HTTPStatus.UNSUPPORTED_MEDIA_TYPE
                return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        writer = ServerResponseWriter()

        handle_task = asyncio.create_task(self._handle_rpc(request, response_headers, response_trailers, writer))
        writer_task = asyncio.create_task(writer.receive())

        response: Response | None = None
        try:
            done, _ = await asyncio.wait(
                [handle_task, writer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if handle_task in done:
                exc = handle_task.exception()
                if exc:
                    raise exc

            if writer_task in done:
                response = writer_task.result()

        except asyncio.CancelledError:
            raise

        finally:
            tasks = [handle_task, writer_task]
            for t in tasks:
                if not t.done():
                    t.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        if not response:
            response = PlainTextResponse(content="Internal Server Error", status_code=500)

        return response

    async def _handle_rpc(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> None:
        """Handles a single RPC request.

        This internal method orchestrates the processing of a request by:
        1. Initializing a protocol-specific connection handler.
        2. Parsing the request timeout.
        3. Executing the user-provided service implementation within the timeout.
        4. Catching any exceptions, including timeouts, and mapping them to
           the appropriate Connect protocol error.
        5. Sending the error back to the client if one occurs.

        Args:
            request: The incoming request object.
            response_headers: The headers for the response.
            response_trailers: The trailers for the response.
            writer: The server response writer to send data to the client.
        """
        conn = await self.protocol_handler.conn(request, response_headers, response_trailers, writer)
        if conn is None:
            return

        try:
            timeout = conn.parse_timeout()
            if timeout:
                with anyio.fail_after(delay=timeout):
                    await self.implementation(conn, timeout)
            else:
                await self.implementation(conn, None)

        except Exception as e:
            if isinstance(e, TimeoutError):
                error = ConnectError("the operation timed out", Code.DEADLINE_EXCEEDED)

            elif isinstance(e, NotImplementedError):
                error = ConnectError("not implemented", Code.UNIMPLEMENTED)

            else:
                error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)

            await conn.send_error(error)


class UnaryHandler[T_Request, T_Response](Handler):
    """A concrete implementation of the `Handler` class for unary RPCs.

    This handler is responsible for processing RPCs that involve a single request message
    and a single response message. It is generic over the request and response types.

    Attributes:
        stream_type (StreamType): The type of stream, fixed to `StreamType.Unary`.
        input (type[T_Request]): The type of the input request message.
        output (type[T_Response]): The type of the output response message.
        call (UnaryFunc[T_Request, T_Response]): The asynchronous function that implements the RPC logic,
            potentially wrapped with interceptors.
    """

    stream_type: StreamType = StreamType.Unary
    input: type[T_Request]
    output: type[T_Response]
    call: UnaryFunc[T_Request, T_Response]

    def __init__(
        self,
        procedure: str,
        unary: UnaryFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],
        options: HandlerOptions | None = None,
    ) -> None:
        """Initializes a new unary handler.

        This sets up the necessary components for handling a unary RPC call,
        including protocol-specific handlers (Connect, gRPC, gRPC-Web) and
        any configured interceptors.

        Args:
            procedure: The full name of the procedure, e.g., "/package.Service/Method".
            unary: The asynchronous function that implements the RPC logic.
            input: The type of the request message.
            output: The type of the response message.
            options: Optional configuration for the handler, including interceptors.
        """
        options = options if options is not None else HandlerOptions()

        config = HandlerConfig(procedure=procedure, stream_type=StreamType.Unary, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _call(request: UnaryRequest[T_Request], context: HandlerContext) -> UnaryResponse[T_Response]:
            response = await unary(request, context)

            return response

        call = apply_interceptors(_call, options.interceptors)

        self.input = input
        self.output = output
        self.call = call

        super().__init__(
            procedure=procedure,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """Implementation of the unary handler.

        This method orchestrates the handling of a single incoming request. It
        receives the request message, invokes the user-defined RPC logic via
        `self.call`, and sends back the resulting response message. It also
        propagates headers and trailers from the response to the connection.

        Args:
            conn (StreamingHandlerConn): The connection object for the stream.
            timeout (float | None): The timeout for the request, in seconds.
        """
        request = await receive_unary_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(exclude_protocol_headers(response.headers))
        conn.response_trailers.update(exclude_protocol_headers(response.trailers))

        await conn.send(aiterate([response.message]))


class ServerStreamHandler[T_Request, T_Response](Handler):
    """Handler for server-streaming RPCs.

    This class manages the lifecycle of a server-streaming RPC. It is responsible for
    receiving a single request message from the client, invoking the user-defined stream
    function to generate a stream of response messages, and sending these messages back
    to the client.

    It is generic over the request type `T_Request` and the response type `T_Response`.

    Attributes:
        stream_type (StreamType): The type of stream, always `StreamType.ServerStream`.
        input (type[T_Request]): The protobuf message type for the request.
        output (type[T_Response]): The protobuf message type for the response.
        call (StreamFunc[T_Request, T_Response]): The wrapped, user-provided stream function,
            including any configured interceptors.
    """

    stream_type: StreamType = StreamType.ServerStream
    input: type[T_Request]
    output: type[T_Response]
    call: StreamFunc[T_Request, T_Response]

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],
        options: HandlerOptions | None = None,
    ) -> None:
        """Initializes a new server streaming handler.

        Args:
            procedure (str): The full name of the procedure, e.g., /my.service.v1.MyService/MyMethod.
            stream (StreamFunc[T_Request, T_Response]): The async function that implements the server
                streaming logic. It takes a request stream and a context, and returns a response stream.
            input (type[T_Request]): The type of the request message.
            output (type[T_Response]): The type of the response message.
            options (HandlerOptions | None, optional): Optional configuration for the handler.
                Defaults to None.
        """
        options = options if options is not None else HandlerOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.ServerStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _call(request: StreamRequest[T_Request], context: HandlerContext) -> StreamResponse[T_Response]:
            response = await stream(request, context)
            return response

        call = apply_interceptors(_call, options.interceptors)

        self.input = input
        self.output = output
        self.call = call

        super().__init__(
            procedure=procedure,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """Handles the logic for a streaming RPC.

        This method orchestrates the handling of a streaming request. It receives
        the request data, invokes the user-defined service logic via the `call`
        method, and sends the resulting response back to the client. It also
        manages the transfer of headers and trailers.

        Args:
            conn (StreamingHandlerConn): The connection object representing the
                bidirectional stream with the client.
            timeout (float | None): An optional timeout in seconds for the handler's
                execution.
        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)


class ClientStreamHandler[T_Request, T_Response](Handler):
    """A handler for client streaming RPCs.

    This handler manages RPCs where the client sends a stream of messages and the
    server responds with a single message. It orchestrates receiving the client's
    stream, invoking the user-defined implementation, and sending the final
    response.

    Attributes:
        stream_type (StreamType): The type of stream, always `StreamType.ClientStream`.
        input (type[T_Request]): The protobuf message class for the request.
        output (type[T_Response]): The protobuf message class for the response.
        call (StreamFunc[T_Request, T_Response]): The wrapped, interceptor-aware
            asynchronous function that implements the RPC logic.
    """

    stream_type: StreamType = StreamType.ClientStream
    input: type[T_Request]
    output: type[T_Response]
    call: StreamFunc[T_Request, T_Response]

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],
        options: HandlerOptions | None = None,
    ) -> None:
        """Initializes a client streaming RPC handler.

        This handler is responsible for processing a client streaming RPC, where the
        client sends a stream of messages and the server responds with a single message.

        Args:
            procedure: The full name of the RPC procedure.
            stream: The asynchronous function that implements the RPC logic.
                It receives a `StreamRequest` (an async iterator of request
                messages) and a `HandlerContext`, and returns a `StreamResponse`
                containing the single response message.
            input: The protobuf message class for the request.
            output: The protobuf message class for the response.
            options: Optional configuration for the handler, including interceptors.
        """
        options = options if options is not None else HandlerOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.ClientStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _call(request: StreamRequest[T_Request], context: HandlerContext) -> StreamResponse[T_Response]:
            response = await stream(request, context)
            return response

        call = apply_interceptors(_call, options.interceptors)

        self.input = input
        self.output = output
        self.call = call

        super().__init__(
            procedure=procedure,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """The core implementation for the streaming handler.

        This method orchestrates the handling of a streaming request. It receives
        the request from the connection, invokes the user-defined call logic
        with a context object, and sends the resulting response headers,
        trailers, and messages back to the client.

        Args:
            conn (StreamingHandlerConn): The connection object for the streaming RPC,
                used for receiving the request and sending the response.
            timeout (float | None): The maximum time in seconds to allow for the
                handler's execution.
        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)

        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)


class BidiStreamHandler[T_Request, T_Response](Handler):
    """Handler for bidirectional streaming procedures.

    This class manages the lifecycle of a bidirectional streaming RPC, where both the client
    and the server can send a stream of messages to each other. It wraps the user-provided
    stream function with necessary protocol logic and interceptors.

    Generic Types:
        T_Request: The type of the request messages.
        T_Response: The type of the response messages.

    Attributes:
        stream_type (StreamType): The type of stream, set to BiDiStream.
        input (type[T_Request]): The expected type for request messages.
        output (type[T_Response]): The expected type for response messages.
        call (StreamFunc[T_Request, T_Response]): The wrapped, user-provided stream function
            that processes the request and generates the response.
    """

    stream_type: StreamType = StreamType.BiDiStream
    input: type[T_Request]
    output: type[T_Response]
    call: StreamFunc[T_Request, T_Response]

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],
        options: HandlerOptions | None = None,
    ) -> None:
        """Initializes a bi-directional streaming handler.

        Args:
            procedure: The full name of the procedure (e.g., /acme.foo.v1.FooService/Bar).
            stream: The async function that implements the bi-directional stream logic.
            input: The type of the request message.
            output: The type of the response message.
            options: Handler-specific options.
        """
        options = options if options is not None else HandlerOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.BiDiStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _call(request: StreamRequest[T_Request], context: HandlerContext) -> StreamResponse[T_Response]:
            response = await stream(request, context)
            return response

        call = apply_interceptors(_call, options.interceptors)

        self.input = input
        self.output = output
        self.call = call

        super().__init__(
            procedure=procedure,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """Handles the logic for a streaming RPC.

        This method orchestrates the handling of a streaming request. It receives
        the request data from the connection, invokes the user-defined `call`
        method with the request and a context object, and then sends the
        resulting response, including headers and trailers, back to the client.

        Args:
            conn (StreamingHandlerConn): The connection object representing the
                bidirectional stream with the client.
            timeout (float | None): An optional timeout in seconds for the
                handler's execution.
        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)
