import abc
from enum import Enum
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel

from connect.request import ConnectRequest


class StreamType(Enum):
    """Enum for the type of stream."""

    Unary = "Unary"
    ClientStream = "ClientStream"
    ServerStream = "ServerStream"
    BiDiStream = "BiDiStream"


class Spec(BaseModel):
    """Spec class."""

    stream_type: StreamType


class StreamingHandlerConn(abc.ABC):
    @abc.abstractmethod
    def spec(self) -> Spec:
        pass

    @abc.abstractmethod
    def peer(self) -> Any:
        pass

    @abc.abstractmethod
    def receive(self, message: Any) -> Any:
        pass

    @abc.abstractmethod
    def request_header(self) -> Any:
        pass

    @abc.abstractmethod
    def send(self, message: Any) -> bytes:
        pass

    @abc.abstractmethod
    def response_header(self) -> Any:
        pass

    @abc.abstractmethod
    def response_trailer(self) -> Any:
        pass


class ReceiveConn(Protocol):
    def spec(self) -> Spec: ...

    def receive(self, message: Any) -> Any: ...


T = TypeVar("T")


def receive_unary_request(conn: StreamingHandlerConn, t: type[T]) -> ConnectRequest[T]:
    message = receive_unary_message(conn, t)
    return ConnectRequest(message)


def receive_unary_message(conn: ReceiveConn, t: type[T]) -> T:
    message = conn.receive(t)
    return message
