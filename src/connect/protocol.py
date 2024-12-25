import abc
from enum import Enum

from pydantic import BaseModel

from connect.connect import Spec


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

    spec: Spec


class ProtocolHandler(abc.ABC):
    """ProtocolHandler class."""

    @abc.abstractmethod
    def methods(self) -> list[HttpMethod]:
        pass

    @abc.abstractmethod
    def content_types(self) -> None:
        pass

    def can_handle_payload(self) -> bool:
        raise NotImplementedError


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
