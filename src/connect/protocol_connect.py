from typing import Any

from connect.codec import Codec
from connect.connect import StreamType
from connect.protocol import HttpMethod, Protocol, ProtocolHandler, ProtocolHandlerParams
from connect.request import Request

CONNECT_UNARY_HEADER_COMPRESSION = "content-Encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "accept-Encoding"
CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"


CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"


def connect_codec_from_content_type(stream_type: StreamType, content_type: str) -> str:
    if stream_type == StreamType.Unary:
        return content_type[len(CONNECT_UNARY_CONTENT_TYPE_PREFIX) :]

    return content_type[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]


class ConnectHandler(ProtocolHandler):
    params: ProtocolHandlerParams
    __methods: list[HttpMethod]
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HttpMethod], accept: list[str]) -> None:
        self.params = params
        self.__methods = methods
        self.accept = accept

    def methods(self) -> list[HttpMethod]:
        return self.__methods

    def content_types(self) -> None:
        pass

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Check if the handler can handle the payload."""
        if HttpMethod(request.method) == HttpMethod.GET:
            pass

        return content_type in self.accept

    async def conn(self, request: Request) -> None:
        query = request.url.query
        if self.params.spec.stream_type == StreamType.Unary:
            if HttpMethod(request.method) == HttpMethod.GET:
                # TODO(tsubakiky): Get the compression from the query parameter
                pass
            else:
                content_encoding = request.headers.get(CONNECT_UNARY_HEADER_COMPRESSION, "")

            accept_encoding = request.headers.get(CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION, "")
        # TODO(tsubakiky): Add validations

        if HttpMethod(request.method) == HttpMethod.GET:
            pass
        else:
            request_body = await request.body()
            content_type = request.headers.get("content-type", "")
            codec_name = connect_codec_from_content_type(self.params.spec.stream_type, content_type)

        codec = self.params.codecs.get(codec_name)
        if self.params.spec.stream_type == StreamType.Unary:
            pass
        else:
            # TODO(tsubakiky): Add streaming support
            pass
        pass


class ProtocolConnect(Protocol):
    def __init__(self) -> None:
        pass

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        methods = [HttpMethod.POST]

        if params.spec.stream_type == StreamType.Unary:
            methods.append(HttpMethod.GET)

        content_types: list[str] = []
        for name in params.codecs.names():
            if params.spec.stream_type == StreamType.Unary:
                content_types.append(CONNECT_UNARY_CONTENT_TYPE_PREFIX + name)
                continue

            content_types.append(CONNECT_STREAMING_CONTENT_TYPE_PREFIX + name)

        return ConnectHandler(params, methods=methods, accept=content_types)

    def client(self) -> None:
        pass


class ConnectUnmarshaler:
    codec: Codec

    def __init__(self, codec: Codec) -> None:
        self.codec = codec

    def unmarshal(self, message: Any) -> None:
        pass
