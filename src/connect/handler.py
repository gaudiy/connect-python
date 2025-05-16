"""Module provides handler configurations and implementations for unary procedures and stream types."""

import asyncio
from collections.abc import Awaitable, Callable
from http import HTTPMethod, HTTPStatus
from typing import Any

import anyio
from starlette.responses import PlainTextResponse, Response

from connect.code import Code
from connect.codec import Codec, CodecMap, CodecNameType, ProtoBinaryCodec, ProtoJSONCodec
from connect.compression import Compression, GZipCompression
from connect.connect import (
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
from connect.error import ConnectError
from connect.handler_context import HandlerContext
from connect.handler_interceptor import apply_interceptors
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.options import ConnectOptions
from connect.protocol import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    ProtocolHandler,
    ProtocolHandlerParams,
    exclude_protocol_headers,
    mapped_method_handlers,
    sorted_accept_post_value,
    sorted_allow_method_value,
)
from connect.protocol_connect.connect_protocol import (
    ProtocolConnect,
)
from connect.protocol_grpc.grpc_protocol import ProtocolGRPC
from connect.request import Request
from connect.response_writer import ServerResponseWriter
from connect.utils import aiterate

type UnaryFunc[T_Request, T_Response] = Callable[
    [UnaryRequest[T_Request], HandlerContext], Awaitable[UnaryResponse[T_Response]]
]
type StreamFunc[T_Request, T_Response] = Callable[
    [StreamRequest[T_Request], HandlerContext], Awaitable[StreamResponse[T_Response]]
]


class HandlerConfig:
    """HandlerConfig encapsulates the configuration for a handler in the Connect framework.

    Attributes:
        codecs (dict[str, Codec]): A mapping of codec names to codec instances supported by the handler.
        compressions (list[Compression]): A list of compression algorithms supported by the handler.
        descriptor (Any): The descriptor providing metadata about the procedure.
        compress_min_bytes (int): The minimum message size (in bytes) before compression is applied.
        read_max_bytes (int): The maximum number of bytes allowed to be read in a single message.
        send_max_bytes (int): The maximum number of bytes allowed to be sent in a single message.
        require_connect_protocol_header (bool): Whether the Connect protocol header is required.
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

    def __init__(self, procedure: str, stream_type: StreamType, options: ConnectOptions):
        """Initialize a new handler instance with the specified procedure, stream type, and options.

        Args:
            procedure (str): The name of the procedure to handle.
            stream_type (StreamType): The type of stream (e.g., unary, server streaming, etc.).
            options (ConnectOptions): Configuration options for the handler, including descriptor, compression, and protocol settings.

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
        """Create and returns a Spec object initialized with the current handler's procedure, descriptor, stream type, and idempotency level.

        Returns:
            Spec: An instance of the Spec class containing the handler's configuration.

        """
        return Spec(
            procedure=self.procedure,
            descriptor=self.descriptor,
            stream_type=self.stream_type,
            idempotency_level=self.idempotency_level,
        )


def create_protocol_handlers(config: HandlerConfig) -> list[ProtocolHandler]:
    """Create and returns a list of protocol handlers based on the provided configuration.

    Args:
        config (HandlerConfig): The configuration object containing settings for codecs, compressions,
            byte limits, protocol requirements, and idempotency level.

    Returns:
        list[ProtocolHandler]: A list of initialized protocol handler instances for each supported protocol.

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
    """Handler is an abstract base class for handling HTTP requests in a protocol-agnostic way, supporting both unary and streaming RPCs.

    Attributes:
        protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): Mapping of HTTP methods to their protocol handlers.
        allow_methods (str): String specifying allowed HTTP methods.
        accept_post (str): String specifying accepted content types for POST requests.
        protocol_handler (ProtocolHandler): The protocol handler selected for the current request.

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
        """Initialize the handler with the specified procedure, protocol handlers, and HTTP method configurations.

        Args:
            procedure (str): The name of the procedure to be handled.
            protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A mapping of HTTP methods to their corresponding protocol handlers.
            allow_methods (str): A string specifying which HTTP methods are allowed.
            accept_post (str): A string specifying the accepted content types for POST requests.

        """
        self.procedure = procedure
        self.protocol_handlers = protocol_handlers
        self.allow_methods = allow_methods
        self.accept_post = accept_post

    async def implementation(self, conn: StreamingHandlerConn, timeout: float | None) -> None:
        """Abstract method to be implemented by subclasses to handle streaming connections.

        Args:
            conn (StreamingHandlerConn): The streaming connection handler instance.
            timeout (float | None): Optional timeout value in seconds for the operation.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        """
        raise NotImplementedError()

    async def handle(self, request: Request) -> Response:
        """Handle an incoming HTTP request and returns an appropriate response.

        This method determines the correct protocol handler based on the HTTP method and content type,
        validates the request (including checking for unsupported methods or media types), and manages
        asynchronous processing of the request and response writing.

        Args:
            request (Request): The incoming HTTP request to be handled.

        Returns:
            Response: The HTTP response generated by the handler.

        Raises:
            Exception: Propagates any exception raised during request handling.
            asyncio.CancelledError: If the handling task is cancelled.

        Behavior:
            - Returns 405 Method Not Allowed if the HTTP method is not supported.
            - Returns 415 Unsupported Media Type if no protocol handler can handle the request's content type.
            - For GET requests, returns 415 if a request body is present.
            - Handles the request asynchronously, ensuring proper cleanup of tasks.
            - Returns a 500 Internal Server Error if no response is generated.

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

        handle_task = asyncio.create_task(self._handle(request, response_headers, response_trailers, writer))
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

    async def _handle(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> None:
        """Handle an incoming request by establishing a connection, parsing timeout values, and invoking the implementation logic.

        Args:
            request (Request): The incoming request object.
            response_headers (Headers): Headers to be sent in the response.
            response_trailers (Headers): Trailers to be sent in the response.
            writer (ServerResponseWriter): The writer used to send responses to the client.

        Returns:
            None

        Raises:
            Sends an appropriate ConnectError to the client if an exception occurs during processing, including timeout, unimplemented, or internal errors.

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
    """UnaryHandler is a generic handler class for unary RPC procedures.

    Type Parameters:
        T_Request: The type of the request message.
        T_Response: The type of the response message.

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
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a handler for a unary RPC procedure.

        Args:
            procedure (str): The name of the RPC procedure.
            unary (UnaryFunc[T_Request, T_Response]): The asynchronous function implementing the unary RPC logic.
            input (type[T_Request]): The expected input type for the request.
            output (type[T_Response]): The expected output type for the response.
            options (ConnectOptions | None, optional): Optional configuration for the handler, such as interceptors. Defaults to None.

        Calls the superclass initializer with the configured protocol handlers and method options.

        """
        options = options if options is not None else ConnectOptions()

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
        """Handle the implementation of a streaming handler connection.

        This asynchronous method receives a unary request from the given connection,
        optionally sets a timeout on the request, invokes the handler's call method,
        and sends the response message back through the connection. It also updates
        the connection's response headers and trailers, excluding protocol-specific headers.

        Args:
            conn (StreamingHandlerConn): The streaming handler connection to process.
            timeout (float | None): Optional timeout value to set on the request.

        Returns:
            None

        """
        request = await receive_unary_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(exclude_protocol_headers(response.headers))
        conn.response_trailers.update(exclude_protocol_headers(response.trailers))

        await conn.send(aiterate([response.message]))


class ServerStreamHandler[T_Request, T_Response](Handler):
    """ServerStreamHandler is a handler class for server-streaming RPC procedures.

    This generic class manages the lifecycle and protocol handling for server-streaming RPCs,
    where a single request from the client results in a stream of responses from the server.
    It sets up protocol handlers, applies interceptors, and provides an asynchronous implementation
    method to process incoming streaming requests and send responses.

    Type Parameters:
        T_Request: The type of the request message.
        T_Response: The type of the response message.

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
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a server-streaming handler for a given procedure.

        Args:
            procedure (str): The name of the RPC procedure.
            stream (StreamFunc[T_Request, T_Response]): The asynchronous stream function handling the server-streaming logic.
            input (type[T_Request]): The expected request message type.
            output (type[T_Response]): The expected response message type.
            options (ConnectOptions | None, optional): Additional configuration options for the handler. Defaults to None.

        Raises:
            Any exceptions raised by the parent class initializer.

        """
        options = options if options is not None else ConnectOptions()
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
        """Handle the implementation of a streaming handler.

        This asynchronous method receives a stream request, optionally sets a timeout,
        invokes the handler's call method, updates the connection's response headers and trailers,
        and sends the response messages through the connection.

        Args:
            conn (StreamingHandlerConn): The streaming connection handler.
            timeout (float | None): Optional timeout value for the request in seconds.

        Returns:
            None

        Raises:
            Any exceptions raised by `receive_stream_request`, `self.call`, or `conn.send` will propagate.

        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)


class ClientStreamHandler[T_Request, T_Response](Handler):
    """ClientStreamHandler is a handler class for client-streaming RPC procedures.

    This generic class manages the lifecycle of a client-streaming RPC, including request handling,
    stream invocation, interceptor application, and response transmission. It is parameterized by
    the request and response message types.

    Type Parameters:
        T_Request: The type of the input message for the stream.
        T_Response: The type of the output message for the stream.

        stream_type (StreamType): The type of stream handled (ClientStream).
        call (StreamFunc[T_Request, T_Response]): The wrapped stream call function with applied interceptors.

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
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a handler for a client-streaming RPC procedure.

        Args:
            procedure (str): The name of the RPC procedure.
            stream (StreamFunc[T_Request, T_Response]): The asynchronous stream function handling the client-streaming logic.
            input (type[T_Request]): The expected input message type.
            output (type[T_Response]): The expected output message type.
            options (ConnectOptions | None, optional): Additional configuration options for the handler. Defaults to None.

        Raises:
            Any exceptions raised by the parent class initializer or protocol handler creation.

        """
        options = options if options is not None else ConnectOptions()
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
        """Handle the implementation of a streaming handler.

        This asynchronous method receives a streaming request, optionally sets a timeout,
        calls the handler logic, updates the connection's response headers and trailers,
        and sends the response messages back through the connection.

        Args:
            conn (StreamingHandlerConn): The streaming connection handler.
            timeout (float | None): Optional timeout value for the request in seconds.

        Returns:
            None

        Raises:
            Any exceptions raised by `receive_stream_request` or `self.call` will propagate.

        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)

        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)


class BidiStreamHandler[T_Request, T_Response](Handler):
    """BidiStreamHandler is a handler class for bidirectional streaming procedures.

    This generic class manages the lifecycle of a bidirectional streaming RPC, including request/response type validation,
    application of interceptors, and integration with protocol-specific handlers. It wraps the provided stream function,
    applies any configured interceptors, and exposes an asynchronous implementation method to process streaming requests
    and send responses.

    Type Parameters:
        T_Request: The type of the request messages.
        T_Response: The type of the response messages.

        stream_type (StreamType): The type of stream handled (always StreamType.BiDiStream).
        call (StreamFunc[T_Request, T_Response]): The wrapped stream call function with applied interceptors.

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
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a handler for a bidirectional streaming procedure.

        Args:
            procedure (str): The name of the procedure to handle.
            stream (StreamFunc[T_Request, T_Response]): The asynchronous stream function handling requests and responses.
            input (type[T_Request]): The expected input type for requests.
            output (type[T_Response]): The expected output type for responses.
            options (ConnectOptions | None, optional): Configuration options for the handler. Defaults to None.

        Raises:
            Any exceptions raised by the parent class initializer.

        """
        options = options if options is not None else ConnectOptions()
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
        """Handle the implementation of a streaming handler.

        This asynchronous method receives a streaming request, optionally sets a timeout,
        calls the main processing function, updates the connection's response headers and trailers,
        and sends the response messages back through the connection.

        Args:
            conn (StreamingHandlerConn): The streaming connection handler.
            timeout (float | None): Optional timeout value for the request in seconds.

        Returns:
            None

        Raises:
            Any exceptions raised by `receive_stream_request` or `self.call` will propagate.

        """
        request = await receive_stream_request(conn, self.input)
        context = HandlerContext(timeout=timeout)
        response = await self.call(request, context)

        conn.response_headers.update(response.headers)
        conn.response_trailers.update(response.trailers)

        await conn.send(response.messages)
