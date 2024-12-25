"""Handler module."""

from collections.abc import Awaitable, Callable
from typing import Any

from connect.connect import Spec, StreamType
from connect.options import ConnectOptions
from connect.protocol import HttpMethod, ProtocolHandler, ProtocolHandlerParams, mapped_method_handlers
from connect.protocol_connect import ProtocolConnect
from connect.request import ConnectRequest, Req, Request
from connect.response import ConnectResponse, Res

UnaryFunc = Callable[[ConnectRequest[Req]], Awaitable[ConnectResponse[Res]]]


class HandlerConfig:
    procedure: str
    stream_type: StreamType

    def __init__(self, procedure: str, stream_type: StreamType, options: Any | None = None):
        self.procedure = procedure
        self.stream_type = stream_type

    def spec(self) -> Spec:
        return Spec(stream_type=self.stream_type)

    def protocol_handlers(self) -> list[ProtocolHandler]:
        protocols = [ProtocolConnect()]

        handlers: list[ProtocolHandler] = []
        for protocol in protocols:
            handlers.append(protocol.handler(params=ProtocolHandlerParams(spec=self.spec())))

        return handlers


class UnaryHandler:
    """UnaryHandler class."""

    protocol_handlers: dict[HttpMethod, list[ProtocolHandler]]

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

    async def serve(self, request: Request) -> bytes:
        """Serve the unary handler."""
        connect_request = await ConnectRequest.from_request(self.input, request)
        response = await self.unary(connect_request)
        res_bytes = response.encode(content_type=request.headers.get("content-type"))
        return res_bytes
