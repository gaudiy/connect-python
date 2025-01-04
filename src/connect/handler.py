"""Module provides handler configurations and implementations for unary procedures and stream types."""

import http
from collections.abc import Awaitable, Callable
from http import HTTPMethod
from typing import Any

import anyio
from starlette.datastructures import MutableHeaders
from starlette.responses import PlainTextResponse

from connect.code import Code
from connect.codec import Codec, CodecMap, CodecNameType, ProtoBinaryCodec, ProtoJSONCodec
from connect.compression import Compression, GZipCompression
from connect.connect import (
    ConnectRequest,
    ConnectResponse,
    Req,
    Res,
    Spec,
    StreamingHandlerConn,
    StreamType,
    receive_unary_request,
)
from connect.error import ConnectError
from connect.interceptor import apply_interceptors
from connect.options import ConnectOptions
from connect.protocol import (
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    ProtocolHandler,
    ProtocolHandlerParams,
    mapped_method_handlers,
    sorted_accept_post_value,
    sorted_allow_method_value,
)
from connect.protocol_connect import (
    CONNECT_HEADER_TIMEOUT,
    CONNECT_UNARY_CONTENT_TYPE_JSON,
    CONNECT_UNARY_TRAILER_PREFIX,
    ProtocolConnect,
    connect_code_to_http,
    error_to_json_bytes,
)
from connect.request import Request
from connect.response import Response

UnaryFunc = Callable[[ConnectRequest[Req]], Awaitable[ConnectResponse[Res]]]


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
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    require_connect_protocol_header: bool

    def __init__(self, procedure: str, stream_type: StreamType, _options: Any | None = None):
        """Initialize the handler with the given procedure, stream type, and optional settings.

        Args:
            procedure (str): The name of the procedure to handle.
            stream_type (StreamType): The type of stream to use.
            _options (Any, optional): Additional options for the handler. Defaults to None.

        """
        self.procedure = procedure
        self.stream_type = stream_type

        codecs: list[Codec] = [
            ProtoBinaryCodec(),
            ProtoJSONCodec(CodecNameType.JSON),
            ProtoJSONCodec(CodecNameType.JSON_CHARSET_UTF8),
        ]
        self.codecs = {codec.name(): codec for codec in codecs}

        self.compressions = [GZipCompression()]
        self.compress_min_bytes = -1
        self.read_max_bytes = -1
        self.send_max_bytes = -1
        self.require_connect_protocol_header = False

    def spec(self) -> Spec:
        """Return a Spec object initialized with the current stream type.

        Returns:
            Spec: An instance of the Spec class with the stream type set to the current stream type.

        """
        return Spec(stream_type=self.stream_type)


def create_protocol_handlers(config: HandlerConfig) -> list[ProtocolHandler]:
    """Create a list of protocol handlers based on the given configuration.

    Args:
        config (HandlerConfig): The configuration object containing the necessary parameters
                                such as codecs and protocol specifications.

    Returns:
        list[ProtocolHandler]: A list of initialized protocol handlers.

    """
    protocols = [ProtocolConnect()]

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
                )
            )
        )

    return handlers


class UnaryHandler:
    """A handler for unary RPC (Remote Procedure Call) operations.

    Attributes:
        protocol_handlers (dict[HTTPMethod, list[ProtocolHandler]]): A dictionary mapping HTTP methods to lists of protocol handlers.
        procedure (str): The name of the procedure being handled.
        unary (UnaryFunc[Req, Res]): The unary function to be executed.
        input (type[Req]): The type of the request input.
        output (type[Res]): The type of the response output.
        options (ConnectOptions | None): Optional configuration options for the handler.

    """

    implementation: Callable[[StreamingHandlerConn], Awaitable[bytes]]
    protocol_handlers: dict[HTTPMethod, list[ProtocolHandler]]
    allow_methods: str
    accept_post: str

    def __init__(
        self,
        procedure: str,
        unary: UnaryFunc[Req, Res],
        input: type[Req],
        output: type[Res],
        options: ConnectOptions | None = None,
    ):
        """Initialize the unary handler."""
        self.procedure = procedure
        self.unary = unary
        self.input = input
        self.output = output
        self.options = options if options is not None else ConnectOptions(interceptors=[])

        config = HandlerConfig(procedure=self.procedure, stream_type=StreamType.Unary)
        protocol_handlers = create_protocol_handlers(config)
        self.protocol_handlers = mapped_method_handlers(protocol_handlers)

        async def _untyped(request: ConnectRequest[Req]) -> ConnectResponse[Res]:
            response = await self.unary(request)

            return response

        untyped = apply_interceptors(_untyped, self.options.interceptors)

        async def implementation(conn: StreamingHandlerConn) -> bytes:
            request = await receive_unary_request(conn, self.input)
            response = await untyped(request)

            conn.response_header().update(response.headers)
            conn.response_trailer().update(response.trailers)
            return conn.send(response.any())

        self.implementation = implementation
        self.allow_methods = sorted_allow_method_value(protocol_handlers)
        self.accept_post = sorted_accept_post_value(protocol_handlers)

    async def handle(self, request: Request) -> Response:
        """Handle an incoming HTTP request and return an HTTP response.

        Args:
            request (Request): The incoming HTTP request.

        Returns:
            Response: The HTTP response generated after processing the request.

        Raises:
            NotImplementedError: If the HTTP method or content type is not implemented.

        """
        response_headers = MutableHeaders()
        response_trailers = MutableHeaders()

        protocol_handlers = self.protocol_handlers.get(HTTPMethod(request.method))
        if not protocol_handlers:
            response_headers["Allow"] = self.allow_methods
            status = http.HTTPStatus.METHOD_NOT_ALLOWED
            return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        content_type = request.headers.get(HEADER_CONTENT_TYPE, "")

        protocol_handler: ProtocolHandler | None = None
        for handler in protocol_handlers:
            if handler.can_handle_payload(request, content_type):
                protocol_handler = handler
                break

        if not protocol_handler:
            response_headers["Accept-Post"] = self.accept_post
            status = http.HTTPStatus.UNSUPPORTED_MEDIA_TYPE
            return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        if HTTPMethod(request.method) == HTTPMethod.GET:
            content_length = request.headers.get(HEADER_CONTENT_LENGTH, None)
            has_body = False

            if content_length is not None:
                has_body = int(content_length) > 0
            else:
                try:
                    async for chunk in request.stream():
                        if chunk != b"":
                            has_body = True
                        break
                except Exception:
                    pass

            if has_body:
                status = http.HTTPStatus.UNSUPPORTED_MEDIA_TYPE
                return PlainTextResponse(content=status.phrase, headers=response_headers, status_code=status.value)

        status_code = http.HTTPStatus.OK.value
        try:
            timeout = request.headers.get(CONNECT_HEADER_TIMEOUT, None)
            timeout_sec = None
            if timeout is not None:
                try:
                    timeout_ms = int(timeout)
                except ValueError as e:
                    raise ConnectError(f"parse timeout: {str(e)}", Code.INVALID_ARGUMENT) from e

                timeout_sec = timeout_ms / 1000

            with anyio.fail_after(timeout_sec):
                conn = await protocol_handler.conn(request, response_headers, response_trailers)
                body = await self.implementation(conn)

        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)

            if isinstance(e, TimeoutError):
                error = ConnectError("the operation timed out", Code.DEADLINE_EXCEEDED)

            status_code = connect_code_to_http(error.code)

            response_headers[HEADER_CONTENT_TYPE] = CONNECT_UNARY_CONTENT_TYPE_JSON
            response_headers.update(error.metadata)

            body = error_to_json_bytes(error)

        for key, value in response_trailers.items():
            response_headers[CONNECT_UNARY_TRAILER_PREFIX + key] = value

        response = Response(content=body, headers=response_headers, status_code=status_code)

        return response
