"""Module provides handler configurations and implementations for unary procedures and stream types."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from http import HTTPMethod, HTTPStatus
from typing import Any, TypeGuard

import anyio
from starlette.responses import PlainTextResponse

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
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.interceptor import apply_interceptors
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
from connect.protocol_connect import (
    ProtocolConnect,
)
from connect.protocol_grpc import ProtocolGPRC
from connect.request import Request
from connect.response import Response
from connect.utils import aiterate
from connect.writer import ServerResponseWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HandlerConfig:
    """Configuration class for handling procedures and stream types.

    Attributes:
        procedure (str): The name of the procedure to handle.
        stream_type (StreamType): The type of stream to use.
        codecs (dict[str, Codec]): A dictionary of codecs used for encoding and decoding.

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
        """Initialize the handler with the given procedure, stream type, and optional settings.

        Args:
            procedure (str): The name of the procedure to handle.
            stream_type (StreamType): The type of stream to use.
            options (Any, optional): Additional options for the handler. Defaults to None.

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
        """Return a Spec object initialized with the current stream type.

        Returns:
            Spec: An instance of the Spec class with the stream type set to the current stream type.

        """
        return Spec(
            procedure=self.procedure,
            descriptor=self.descriptor,
            stream_type=self.stream_type,
            idempotency_level=self.idempotency_level,
        )


def create_protocol_handlers(config: HandlerConfig) -> list[ProtocolHandler]:
    """Create a list of protocol handlers based on the given configuration.

    Args:
        config (HandlerConfig): The configuration object containing the necessary parameters
                                such as codecs and protocol specifications.

    Returns:
        list[ProtocolHandler]: A list of initialized protocol handlers.

    """
    protocols = [ProtocolConnect(), ProtocolGPRC(web=False)]

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


UnaryImplementationFunc = Callable[[StreamingHandlerConn, float | None], Awaitable[None]]
StreamImplementationFunc = Callable[[StreamingHandlerConn, float | None], Awaitable[None]]


class Handler:
    """A class to handle incoming HTTP requests and generate appropriate HTTP responses.

    Attributes:
        procedure (str): The procedure name.
        implementation (UnaryImplementationFunc | StreamImplementationFunc): The implementation function for handling requests.
        protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A dictionary mapping HTTP methods to protocol handlers.
        allow_methods (str): Allowed HTTP methods.
        accept_post (str): Accepted content types for POST requests.
        protocol_handler (ProtocolHandler): The protocol handler for the current request.

    """

    procedure: str
    implementation: UnaryImplementationFunc | StreamImplementationFunc
    protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]]
    allow_methods: str
    accept_post: str
    protocol_handler: ProtocolHandler

    def __init__(
        self,
        procedure: str,
        implementation: UnaryImplementationFunc | StreamImplementationFunc,
        protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]],
        allow_methods: str,
        accept_post: str,
    ) -> None:
        """Initialize a new handler instance.

        Args:
            procedure (str): The name of the procedure.
            implementation (UnaryImplementationFunc | StreamImplementationFunc): The function implementing the procedure.
            protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A dictionary mapping HTTP methods to protocol handlers.
            allow_methods (str): A string specifying allowed HTTP methods.
            accept_post (str): A string specifying if POST method is accepted.

        """
        self.procedure = procedure
        self.implementation = implementation
        self.protocol_handlers = protocol_handlers
        self.allow_methods = allow_methods
        self.accept_post = accept_post

    async def handle(self, request: Request) -> Response:
        """Handle an incoming HTTP request and return an HTTP response.

        Args:
            request (Request): The incoming HTTP request.

        Returns:
            Response: The HTTP response generated after processing the request.

        Raises:
            NotImplementedError: If the HTTP method or content type is not implemented.

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
                try:
                    async for chunk in request.stream():
                        if chunk:
                            has_body = True
                        break
                except Exception:
                    pass

            if has_body:
                status = HTTPStatus.UNSUPPORTED_MEDIA_TYPE
                return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        writer = ServerResponseWriter()

        main_task = asyncio.create_task(self._handle(request, response_headers, response_trailers, writer))
        writer_task = asyncio.create_task(writer.receive())

        response: Response | None = None
        try:
            done, _ = await asyncio.wait(
                [main_task, writer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if main_task in done:
                exc = main_task.exception()
                if exc:
                    raise exc

            if writer_task in done:
                response = writer_task.result()

        except asyncio.CancelledError:
            raise

        finally:
            for t in (main_task, writer_task):
                if not t.done():
                    t.cancel()

            await asyncio.gather(main_task, writer_task, return_exceptions=True)

        if not response:
            response = PlainTextResponse(content="Internal Server Error", status_code=500)

        return response

    async def _handle(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> None:
        # Check the stream type of the handler
        if getattr(self, "stream_type", StreamType.Unary) != StreamType.Unary:
            self._is_stream_handler = True
            await self.stream_handle(request, response_headers, response_trailers, writer)
        else:
            self._is_stream_handler = False
            await self.unary_handle(request, response_headers, response_trailers, writer)

    def is_stream(
        self, impl: UnaryImplementationFunc | StreamImplementationFunc
    ) -> TypeGuard[StreamImplementationFunc]:
        """Determine if the given implementation function is a stream implementation.

        Args:
            impl (UnaryImplementationFunc | StreamImplementationFunc): The implementation function to check.

        Returns:
            TypeGuard[StreamImplementationFunc]: True if the implementation function is a stream implementation, False otherwise.

        """
        # Since we've consolidated to a single connection type, use a sentinel value in the handler config
        is_stream_handler = getattr(self, "_is_stream_handler", False)
        return is_stream_handler

    def is_unary(self, impl: UnaryImplementationFunc | StreamImplementationFunc) -> TypeGuard[UnaryImplementationFunc]:
        """Determine if the given implementation function is a unary implementation.

        Args:
            impl (UnaryImplementationFunc | StreamImplementationFunc): The implementation function to check.

        Returns:
            TypeGuard[UnaryImplementationFunc]: True if the implementation function is a unary implementation, False otherwise.

        """
        # Since we've consolidated to a single connection type, use a sentinel value in the handler config
        is_stream_handler = getattr(self, "_is_stream_handler", False)
        return not is_stream_handler

    async def stream_handle(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> None:
        """Handle streaming requests.

        Args:
            request (Request): The incoming request object.
            response_headers (Headers): The headers to be sent in the response.
            response_trailers (Headers): The trailers to be sent in the response.
            writer (ServerResponseWriter): The writer used to send the response.

        Returns:
            None

        Raises:
            ValueError: If the function type for the stream handler is invalid.
            ConnectError: If an internal error occurs during the handling of the stream.

        """
        self._is_stream_handler = True
        conn = await self.protocol_handler.conn(request, response_headers, response_trailers, writer, is_streaming=True)
        if conn is None:
            return

        implementation = self.implementation
        if not self.is_stream(implementation):
            raise ValueError(f"Invalid function type for stream handler: {implementation}")

        try:
            timeout = conn.parse_timeout()
            if timeout:
                timeout_ms = int(timeout * 1000)

                with anyio.fail_after(delay=timeout):
                    await implementation(conn, timeout_ms)
            else:
                await implementation(conn, None)

        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)

            await conn.send_error(error)

    async def unary_handle(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> None:
        """Handle a unary request.

        Args:
            request (Request): The incoming request object.
            response_headers (Headers): The headers for the response.
            response_trailers (Headers): The trailers for the response.
            writer (ServerResponseWriter): The writer to send the response.

        Raises:
            ValueError: If the function type is invalid for unary handler.
            ConnectError: If there is an error parsing the timeout or an internal error occurs.

        Returns:
            None

        """
        self._is_stream_handler = False
        conn = await self.protocol_handler.conn(request, response_headers, response_trailers, writer)
        if conn is None:
            return

        implementation = self.implementation
        if not self.is_unary(implementation):
            raise ValueError(f"Invalid function type for unary handler: {implementation}")

        try:
            timeout = conn.parse_timeout()
            if timeout:
                timeout_ms = int(timeout * 1000)
                with anyio.fail_after(delay=timeout):
                    await implementation(conn, timeout_ms)
            else:
                await implementation(conn, None)

        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)

            if isinstance(e, TimeoutError):
                error = ConnectError("the operation timed out", Code.DEADLINE_EXCEEDED)

            if isinstance(e, NotImplementedError):
                error = ConnectError("not implemented", Code.UNIMPLEMENTED)
            await conn.send_error(error)


type UnaryFunc[T_Request, T_Response] = Callable[[UnaryRequest[T_Request]], Awaitable[UnaryResponse[T_Response]]]
type StreamFunc[T_Request, T_Response] = Callable[[StreamRequest[T_Request]], Awaitable[StreamResponse[T_Response]]]


class UnaryHandler[T_Request, T_Response](Handler):
    """A handler for unary RPC (Remote Procedure Call) operations.

    Attributes:
        protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A dictionary mapping HTTP methods to lists of protocol handlers.
        procedure (str): The name of the procedure being handled.
        unary (UnaryFunc[Req, Res]): The unary function to be executed.
        input (type[Req]): The type of the request input.
        output (type[Res]): The type of the response output.
        options (ConnectOptions | None): Optional configuration options for the handler.

    """

    procedure: str
    protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]]
    allow_methods: str
    accept_post: str
    stream_type: StreamType = StreamType.Unary

    def __init__(
        self,
        procedure: str,
        unary: UnaryFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],
        options: ConnectOptions | None = None,
    ):
        """Initialize the unary handler."""
        options = options if options is not None else ConnectOptions()

        config = HandlerConfig(procedure=procedure, stream_type=StreamType.Unary, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _untyped(request: UnaryRequest[T_Request]) -> UnaryResponse[T_Response]:
            response = await unary(request)

            return response

        untyped = apply_interceptors(_untyped, options.interceptors)

        async def implementation(conn: StreamingHandlerConn, timeout: float | None) -> None:
            request = await receive_unary_request(conn, input)
            if timeout:
                request.timeout = timeout

            response = await untyped(request)

            if not isinstance(response.message, output):
                raise ConnectError(
                    f"expected response of type: {output.__name__}",
                    Code.INTERNAL,
                )

            conn.response_headers.update(exclude_protocol_headers(response.headers))
            conn.response_trailers.update(exclude_protocol_headers(response.trailers))
            await conn.send(aiterate([response.message]))

        super().__init__(
            procedure=procedure,
            implementation=implementation,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )


class ServerStreamHandler[T_Request, T_Response](Handler):
    """A handler for server-side streaming RPCs.

    This handler manages the server-side streaming procedure, including receiving
    requests, processing them, and sending responses.

    Args:
        procedure (str): The name of the procedure being handled.
        stream (StreamFunc[T_Request, T_Response]): The streaming function that processes
            the request and generates the response.
        input (type[T_Request]): The type of the request message.
        output (type[T_Response]): The type of the response message.
        options (ConnectOptions | None, optional): Configuration options for the handler.
            Defaults to None.

    """

    stream_type: StreamType = StreamType.ServerStream

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],  # noqa: ARG002
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a new handler instance.

        Args:
            procedure (str): The name of the procedure to handle.
            stream (StreamFunc[T_Request, T_Response]): The stream function to handle requests and responses.
            input (type[T_Request]): The type of the request message.
            output (type[T_Response]): The type of the response message.
            options (ConnectOptions | None, optional): Additional options for the handler. Defaults to None.

        """
        options = options if options is not None else ConnectOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.ServerStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _untyped(request: StreamRequest[T_Request]) -> StreamResponse[T_Response]:
            response = await stream(request)

            return response

        untyped = apply_interceptors(_untyped, options.interceptors)

        async def implementation(conn: StreamingHandlerConn, timeout: float | None) -> None:
            request = await receive_stream_request(conn, input)
            if timeout:
                request.timeout = timeout

            response = await untyped(request)

            conn.response_headers.update(response.headers)
            conn.response_trailers.update(response.trailers)

            await conn.send(response.messages)

        super().__init__(
            procedure=procedure,
            implementation=implementation,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )


class ClientStreamHandler[T_Request, T_Response](Handler):
    """A handler for client-side streaming RPCs.

    Args:
        procedure (str): The name of the RPC procedure.
        stream (StreamFunc[T_Request, T_Response]): The function that handles the streaming request.
        input (type[T_Request]): The type of the request message.
        output (type[T_Response]): The type of the response message.
        options (ConnectOptions | None, optional): Configuration options for the handler. Defaults to None.

    Methods:
        implementation(conn: StreamingHandlerConn): Handles the streaming connection, receiving requests and sending responses.

    Attributes:
        procedure (str): The name of the RPC procedure.
        implementation (Callable): The function that implements the streaming logic.
        protocol_handlers (Dict): Handlers for different protocols.
        allow_methods (List[str]): Allowed HTTP methods.
        accept_post (List[str]): Accepted POST values.

    """

    stream_type: StreamType = StreamType.ClientStream

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],  # noqa: ARG002
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a new instance of the handler.

        Args:
            procedure (str): The name of the procedure to handle.
            stream (StreamFunc[T_Request, T_Response]): The stream function to handle requests and responses.
            input (type[T_Request]): The type of the request message.
            output (type[T_Response]): The type of the response message.
            options (ConnectOptions | None, optional): Additional options for the handler. Defaults to None.

        Returns:
            None

        """
        options = options if options is not None else ConnectOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.ClientStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _untyped(request: StreamRequest[T_Request]) -> StreamResponse[T_Response]:
            response = await stream(request)

            return response

        untyped = apply_interceptors(_untyped, options.interceptors)

        async def implementation(conn: StreamingHandlerConn, timeout: float | None) -> None:
            request = await receive_stream_request(conn, input)
            if timeout:
                request.timeout = timeout

            response = await untyped(request)

            conn.response_headers.update(response.headers)
            conn.response_trailers.update(response.trailers)

            await conn.send(response.messages)

        super().__init__(
            procedure=procedure,
            implementation=implementation,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )


class BidiStreamHandler[T_Request, T_Response](Handler):
    """A handler for bidirectional streaming RPCs in a Connect-based framework.

    This class facilitates the implementation of bidirectional streaming
    handlers by managing the lifecycle of the stream, applying interceptors,
    and handling protocol-specific details.

    Type Parameters:
        T_Request: The type of the request messages in the stream.
        T_Response: The type of the response messages in the stream.

    Args:
        procedure (str): The name of the RPC procedure being handled.
        stream (StreamFunc[T_Request, T_Response]): The user-defined function
            that processes the bidirectional stream of requests and responses.
        input (type[T_Request]): The type of the request messages.
        output (type[T_Response]): The type of the response messages.
        options (ConnectOptions | None, optional): Configuration options for
            the handler. Defaults to `None`.

    Attributes:
        procedure (str): The name of the RPC procedure.
        implementation (Callable): The internal implementation of the handler
            that manages the streaming connection.
        protocol_handlers (dict): A mapping of protocol-specific handlers.
        allow_methods (list): A sorted list of allowed HTTP methods for the
            handler.
        accept_post (list): A sorted list of accepted POST methods for the
            handler.

    Methods:
        __init__: Initializes the handler with the provided procedure, stream
            function, input/output types, and options.

    """

    stream_type: StreamType = StreamType.BiDiStream

    def __init__(
        self,
        procedure: str,
        stream: StreamFunc[T_Request, T_Response],
        input: type[T_Request],
        output: type[T_Response],  # noqa: ARG002
        options: ConnectOptions | None = None,
    ) -> None:
        """Initialize a bidirectional streaming handler.

        Args:
            procedure (str): The name of the procedure being handled.
            stream (StreamFunc[T_Request, T_Response]): The function to handle the bidirectional stream.
            input (type[T_Request]): The type of the request messages.
            output (type[T_Response]): The type of the response messages.
            options (ConnectOptions | None, optional): Configuration options for the handler. Defaults to None.

        Raises:
            Any exceptions raised during the initialization of protocol handlers or interceptors.

        Notes:
            - This handler is designed for bidirectional streaming communication.
            - Interceptors, if provided, are applied to the untyped stream function.
            - The implementation processes incoming requests, applies the stream function, and sends responses.

        """
        options = options if options is not None else ConnectOptions()
        config = HandlerConfig(procedure=procedure, stream_type=StreamType.BiDiStream, options=options)
        protocol_handlers = create_protocol_handlers(config)

        async def _untyped(request: StreamRequest[T_Request]) -> StreamResponse[T_Response]:
            response = await stream(request)

            return response

        untyped = apply_interceptors(_untyped, options.interceptors)

        async def implementation(conn: StreamingHandlerConn, timeout: float | None) -> None:
            request = await receive_stream_request(conn, input)
            if timeout:
                request.timeout = timeout

            response = await untyped(request)

            conn.response_headers.update(response.headers)
            conn.response_trailers.update(response.trailers)

            await conn.send(response.messages)

        super().__init__(
            procedure=procedure,
            implementation=implementation,
            protocol_handlers=mapped_method_handlers(protocol_handlers),
            allow_methods=sorted_allow_method_value(protocol_handlers),
            accept_post=sorted_accept_post_value(protocol_handlers),
        )
