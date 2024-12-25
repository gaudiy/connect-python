from connect.connect import StreamType
from connect.protocol import HttpMethod, Protocol, ProtocolHandler, ProtocolHandlerParams
from connect.request import Request

CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"


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
        if HttpMethod(request.method) == HttpMethod.GET:
            pass

        return content_type in self.accept


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
