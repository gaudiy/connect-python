from connect.connect import StreamType
from connect.protocol import HttpMethod, Protocol, ProtocolHandler, ProtocolHandlerParams


class ConnectHandler(ProtocolHandler):
    params: ProtocolHandlerParams
    __methods: list[HttpMethod]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HttpMethod]) -> None:
        self.params = params
        self.__methods = methods
        pass

    def methods(self) -> list[HttpMethod]:
        return self.__methods

    def content_types(self) -> None:
        pass

    def can_handle_payload(self) -> bool:
        return True


class ProtocolConnect(Protocol):
    def __init__(self) -> None:
        pass

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        methods = [HttpMethod.POST]

        if params.spec.stream_type == StreamType.Unary:
            methods.append(HttpMethod.GET)

        return ConnectHandler(params, methods=methods)

    def client(self) -> None:
        pass
