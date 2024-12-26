import abc
from enum import Enum

from pydantic import BaseModel, ConfigDict

from connect.codec import ReadOnlyCodecs
from connect.connect import Spec, StreamingHandlerConn
from connect.request import Request

HEADER_CONTENT_TYPE = "content-type"
HEADER_HOST = "host"


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"


class ProtocolHandlerParams(BaseModel):
    """ProtocolHandlerParams class."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    spec: Spec
    codecs: ReadOnlyCodecs


class ProtocolHandler(abc.ABC):
    """ProtocolHandler class."""

    @abc.abstractmethod
    def methods(self) -> list[HttpMethod]:
        pass

    @abc.abstractmethod
    def content_types(self) -> None:
        pass

    @abc.abstractmethod
    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    async def conn(self, request: Request) -> StreamingHandlerConn:
        pass


class Protocol(abc.ABC):
    """Protocol class."""

    @abc.abstractmethod
    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        pass

    @abc.abstractmethod
    def client(self) -> None:
        pass


def mapped_method_handlers(handlers: list[ProtocolHandler]) -> dict[HttpMethod, list[ProtocolHandler]]:
    method_handlers: dict[HttpMethod, list[ProtocolHandler]] = {}
    for handler in handlers:
        for method in handler.methods():
            method_handlers.setdefault(method, []).append(handler)

    return method_handlers
