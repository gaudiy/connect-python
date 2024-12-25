"""Handler module."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from connect.codec import Codec, CodecMap, ProtoBinaryCodec
from connect.connect import Spec, StreamingHandlerConn, StreamType
from connect.options import ConnectOptions
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    HttpMethod,
    ProtocolHandler,
    ProtocolHandlerParams,
    mapped_method_handlers,
)
from connect.protocol_connect import ProtocolConnect
from connect.request import ConnectRequest, Req, Request
from connect.response import ConnectResponse, Res

UnaryFunc = Callable[[ConnectRequest[Req]], Awaitable[ConnectResponse[Res]]]


class HandlerConfig:
    procedure: str
    stream_type: StreamType
    codecs: dict[str, Codec]

    def __init__(self, procedure: str, stream_type: StreamType, options: Any | None = None):
        self.procedure = procedure
        self.stream_type = stream_type

        protp_binary_codec = ProtoBinaryCodec()
        self.codecs = {protp_binary_codec.name(): protp_binary_codec}

    def spec(self) -> Spec:
        return Spec(stream_type=self.stream_type)

    def protocol_handlers(self) -> list[ProtocolHandler]:
        protocols = [ProtocolConnect()]

        codecs = CodecMap(self.codecs)

        handlers: list[ProtocolHandler] = []
        for protocol in protocols:
            handlers.append(protocol.handler(params=ProtocolHandlerParams(spec=self.spec(), codecs=codecs)))

        return handlers


class UnaryHandler:
    """UnaryHandler class."""

    protocol_handlers: dict[HttpMethod, list[ProtocolHandler]]
    implementation: Callable[[StreamingHandlerConn], None]

    def __init__(
        self,
        procedure: str,
        unary: UnaryFunc[Req, Res],
        input: type[Req],
        output: type[Res],
        options: ConnectOptions | None,
    ):
        """Initialize the unary handler."""
        self.procedure = procedure
        self.unary = unary
        self.input = input
        self.output = output
        self.options = options

        config = HandlerConfig(procedure=self.procedure, stream_type=StreamType.Unary)
        self.protocol_handlers = mapped_method_handlers(config.protocol_handlers())

        def implementation(conn: StreamingHandlerConn) -> None:
            pass

        self.implementation = implementation

    async def serve(self, request: Request) -> tuple[bytes, Mapping[str, str]]:
        """Serve the unary handler."""
        headers = request.headers.mutablecopy()
        protocol_handlers = self.protocol_handlers.get(HttpMethod(request.method))
        if not protocol_handlers:
            # TODO(tsubakiky): Add error handling
            raise NotImplementedError(f"Method {request.method} not implemented")

        content_type = request.headers.get(HEADER_CONTENT_TYPE, "")
        protocol_handler: ProtocolHandler | None = None
        for handler in protocol_handlers:
            if handler.can_handle_payload(request, content_type):
                protocol_handler = handler
                break

        if not protocol_handler:
            # TODO(tsubakiky): Add error handling
            raise NotImplementedError(f"Content type {content_type} not implemented")

        await protocol_handler.conn(request)

        connect_request = await ConnectRequest.from_request(self.input, request)
        response = await self.unary(connect_request)
        res_bytes = response.encode(content_type=request.headers.get("content-type"))
        return res_bytes, headers
