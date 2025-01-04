"""Defines the streaming handler connection interfaces and related utilities."""

import abc
from collections.abc import Mapping, MutableMapping
from enum import Enum
from http import HTTPMethod
from typing import Any, Generic, Protocol, TypeVar, cast

from pydantic import BaseModel
from starlette.datastructures import MutableHeaders

from connect.idempotency_level import IdempotencyLevel
from connect.utils import get_callable_attribute


class StreamType(Enum):
    """Enum for the type of stream."""

    Unary = "Unary"
    ClientStream = "ClientStream"
    ServerStream = "ServerStream"
    BiDiStream = "BiDiStream"


class Spec(BaseModel):
    """Spec class."""

    procedure: str
    descriptor: Any
    stream_type: StreamType
    idempotency_level: IdempotencyLevel


class Address(BaseModel):
    """Address class."""

    host: str
    port: int


class Peer(BaseModel):
    """Peer class."""

    address: Address | None
    protocol: str
    query: Mapping[str, str]


Req = TypeVar("Req")


class ConnectRequest(Generic[Req]):
    """ConnectRequest is a class that encapsulates a request with a message, specification, peer, headers, and method.

    Attributes:
        message (Req): The request message.
        _spec (Spec): The specification of the request.
        _peer (Peer): The peer associated with the request.
        _header (Mapping[str, str]): The headers of the request.
        _method (str): The method of the request.

    """

    message: Req
    _spec: Spec
    _peer: Peer
    _header: Mapping[str, str]
    _method: str

    def __init__(self, message: Req, spec: Spec, peer: Peer, header: Mapping[str, str], method: str) -> None:
        """Initialize a new Request instance.

        Args:
            message (Req): The request message.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            header (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.

        Returns:
            None

        """
        self.message = message
        self._spec = spec
        self._peer = peer
        self._header = header
        self._method = method

    def any(self) -> Req:
        """Return the request message."""
        return self.message

    def spec(self) -> Spec:
        """Return the request specification."""
        return self._spec

    def peer(self) -> Peer:
        """Return the request peer."""
        return self._peer

    def header(self) -> Mapping[str, str]:
        """Return the request headers."""
        return self._header

    def method(self) -> str:
        """Return the request method."""
        return self._method


Res = TypeVar("Res")


class ConnectResponse(Generic[Res]):
    """Response class for handling responses."""

    message: Res
    headers: MutableMapping[str, str] = {}
    trailers: MutableMapping[str, str] = {}

    def __init__(self, message: Res) -> None:
        """Initialize the response with a message."""
        self.message = message

    def any(self) -> Res:
        """Return the response message."""
        return self.message


class StreamingHandlerConn(abc.ABC):
    """Abstract base class for a streaming handler connection.

    This class defines the interface for handling streaming connections, including
    methods for specifying the connection, handling peer communication, receiving
    and sending messages, and managing request and response headers and trailers.

    """

    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the specification details.

        Returns:
            Spec: The specification details.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def peer(self) -> Any:
        """Establish a connection to a peer in the network.

        Returns:
            Any: The result of the connection attempt. The exact type and structure
            of the return value will depend on the implementation details.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self, message: Any) -> Any:
        """Receives a message and processes it.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            Any: The result of processing the message.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def request_header(self) -> Any:
        """Generate and return the request header.

        Returns:
            Any: The request header.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message: Any) -> bytes:
        """Send a message and returns the response as bytes.

        Args:
            message (Any): The message to be sent.

        Returns:
            bytes: The response received after sending the message.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def response_header(self) -> MutableHeaders:
        """Retrieve the response header.

        Returns:
            Any: The response header.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def response_trailer(self) -> MutableHeaders:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The return type is not specified as this is a placeholder method.

        """
        raise NotImplementedError()


class ReceiveConn(Protocol):
    """A protocol that defines the methods required for receiving connections."""

    @abc.abstractmethod
    def spec(self) -> Spec:
        """Retrieve the specification for the current object.

        This method should be implemented by subclasses to return an instance
        of the `Spec` class that defines the specification for the object.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            Spec: The specification for the current object.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self, message: Any) -> Any:
        """Receives a message and processes it.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            Any: The result of processing the message.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError()


T = TypeVar("T")


async def receive_unary_request(conn: StreamingHandlerConn, t: type[T]) -> ConnectRequest[T]:
    """Receives a unary request from the given connection and returns a ConnectRequest object.

    Args:
        conn (StreamingHandlerConn): The connection from which to receive the unary request.
        t (type[T]): The type of the message to be received.

    Returns:
        ConnectRequest[T]: A ConnectRequest object containing the received message.

    """
    message = await receive_unary_message(conn, t)

    method = HTTPMethod.POST
    get_http_method = get_callable_attribute(conn, "get_http_method")
    if get_http_method:
        method = cast(HTTPMethod, get_http_method())

    return ConnectRequest(
        message=message,
        spec=conn.spec(),
        peer=conn.peer(),
        header=conn.request_header(),
        method=method.value,
    )


async def receive_unary_message(conn: ReceiveConn, t: type[T]) -> T:
    """Receives a unary message from the given connection.

    Args:
        conn (ReceiveConn): The connection object to receive the message from.
        t (type[T]): The type of the message to be received.

    Returns:
        T: The received message of type T.

    """
    message = await conn.receive(t)
    return message
