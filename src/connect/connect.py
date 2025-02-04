"""Defines the streaming handler connection interfaces and related utilities."""

import abc
from collections.abc import AsyncIterator, Callable, Mapping
from enum import Enum
from http import HTTPMethod
from types import TracebackType
from typing import Any, Protocol, Self, cast

from pydantic import BaseModel

from connect.headers import Headers
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


class ConnectRequest[T]:
    """ConnectRequest is a class that encapsulates a request with a message, specification, peer, headers, and method.

    Attributes:
        message (Req): The request message.
        _spec (Spec): The specification of the request.
        _peer (Peer): The peer associated with the request.
        _headers (Mapping[str, str]): The headers of the request.
        _method (str): The method of the request.

    """

    message: T
    _spec: Spec
    _peer: Peer
    _headers: Headers
    _method: str

    def __init__(
        self,
        message: T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
    ) -> None:
        """Initialize a new Request instance.

        Args:
            message (Req): The request message.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            headers (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.

        Returns:
            None

        """
        self.message = message
        self._spec = (
            spec
            if spec
            else Spec(
                procedure="",
                descriptor=None,
                stream_type=StreamType.Unary,
                idempotency_level=IdempotencyLevel.IDEMPOTENT,
            )
        )
        self._peer = peer if peer else Peer(address=None, protocol="", query={})
        self._headers = headers if headers else Headers()
        self._method = method if method else HTTPMethod.POST.value

    def any(self) -> T:
        """Return the request message."""
        return self.message

    @property
    def spec(self) -> Spec:
        """Return the request specification."""
        return self._spec

    @spec.setter
    def spec(self, value: Spec) -> None:
        """Set the request specification."""
        self._spec = value

    @property
    def peer(self) -> Peer:
        """Return the request peer."""
        return self._peer

    @peer.setter
    def peer(self, value: Peer) -> None:
        """Set the request peer."""
        self._peer = value

    @property
    def headers(self) -> Headers:
        """Return the request headers."""
        return self._headers

    @property
    def method(self) -> str:
        """Return the request method."""
        return self._method

    def set_request_method(self, value: str) -> None:
        """Set the request method."""
        self._method = value


class ConnectResponse[T]:
    """Response class for handling responses."""

    message: T
    headers: Headers
    trailers: Headers

    def __init__(
        self,
        message: T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initialize the response with a message."""
        self.message = message
        self.headers = headers if headers else Headers()
        self.trailers = trailers if trailers else Headers()

    def any(self) -> T:
        """Return the response message."""
        return self.message


class StreamingHandlerConn(abc.ABC):
    """Abstract base class for a streaming handler connection.

    This class defines the interface for handling streaming connections, including
    methods for specifying the connection, handling peer communication, receiving
    and sending messages, and managing request and response headers and trailers.

    """

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the specification details.

        Returns:
            Spec: The specification details.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Establish a connection to a peer in the network.

        Returns:
            Any: The result of the connection attempt. The exact type and structure
            of the return value will depend on the implementation details.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and processes it.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            Any: The result of processing the message.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Generate and return the request headers.

        Returns:
            Any: The request headers.

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

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Retrieve the response headers.

        Returns:
            Any: The response headers.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The return type is not specified as this is a placeholder method.

        """
        raise NotImplementedError()


class AbstractAsyncContextManager(abc.ABC):
    """Abstract base class for an asynchronous context manager."""

    async def __aenter__(self) -> Self:
        """Enter the context manager and return the instance."""
        return self

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager and handle any exceptions that occur."""
        return None


class UnaryClientConn(AbstractAsyncContextManager):
    """Abstract base class for a streaming client connection."""

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the specification details."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Return the peer information."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self, message: Any) -> Any:
        """Receives a message and processes it."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Return the request headers."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Any) -> bytes:
        """Send a message."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Return the response headers."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Return response trailers."""
        raise NotImplementedError()

    @abc.abstractmethod
    def on_request_send(self, fn: Callable[..., Any]) -> None:
        """Handle the request send event."""
        raise NotImplementedError()


class StreamingClientConn(AbstractAsyncContextManager):
    """Abstract base class for a streaming client connection."""

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Return the specification details."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Return the peer information."""
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and processes it."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Return the request headers."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Any) -> bytes:
        """Send a message."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Return the response headers."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Return response trailers."""
        raise NotImplementedError()

    @abc.abstractmethod
    def on_request_send(self, fn: Callable[..., Any]) -> None:
        """Handle the request send event."""
        raise NotImplementedError()


class ReceiveConn(Protocol):
    """A protocol that defines the methods required for receiving connections."""

    @property
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


async def receive_unary_request[T](conn: StreamingHandlerConn, t: type[T]) -> ConnectRequest[T]:
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
        spec=conn.spec,
        peer=conn.peer,
        headers=conn.request_headers,
        method=method.value,
    )


async def recieve_unary_response[T](conn: UnaryClientConn, t: type[T]) -> ConnectResponse[T]:
    """Receive a unary response from a streaming client connection.

    Args:
        conn (StreamingClientConn): The streaming client connection.
        t (type[T]): The type of the expected response message.

    Returns:
        ConnectResponse[T]: The response containing the message, response headers, and response trailers.

    """
    message = await receive_unary_message(conn, t)

    return ConnectResponse(message, conn.response_headers, conn.response_trailers)


async def recieve_stream_response[T](conn: StreamingClientConn, t: type[T]) -> AsyncIterator[ConnectResponse[T]]:
    """Asynchronously receives a stream of responses from a streaming client connection.

    Args:
        conn (StreamingClientConn): The streaming client connection to receive messages from.
        t (type[T]): The type of the messages to be received.

    Yields:
        ConnectResponse[T]: An asynchronous iterator of ConnectResponse objects containing the received messages, response headers, and response trailers.

    Type Parameters:
        T: The type of the messages to be received.

    """
    async for message in conn.receive(t):
        yield ConnectResponse(message, conn.response_headers, conn.response_trailers)


async def receive_unary_message[T](conn: ReceiveConn, t: type[T]) -> T:
    """Receives a unary message from the given connection.

    Args:
        conn (ReceiveConn): The connection object to receive the message from.
        t (type[T]): The type of the message to be received.

    Returns:
        T: The received message of type T.

    """
    message = await conn.receive(t)
    return message
