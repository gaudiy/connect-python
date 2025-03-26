"""Defines the streaming handler connection interfaces and related utilities."""

import abc
from collections.abc import AsyncIterator, Callable, Mapping
from enum import Enum
from http import HTTPMethod
from typing import Any, Protocol, cast

from pydantic import BaseModel

from connect.code import Code
from connect.error import ConnectError
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.utils import aiterate, get_callable_attribute


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


class RequestCommon:
    """RequestCommon is a class that encapsulates common attributes and methods for handling HTTP requests.

    Attributes:
        _spec (Spec): The specification for the request.
        _peer (Peer): The peer information.
        _headers (Headers): The request headers.
        _method (str): The HTTP method used for the request.

    """

    _spec: Spec
    _peer: Peer
    _headers: Headers
    _method: str

    def __init__(
        self,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
    ) -> None:
        """Initialize a new Request instance.

        Args:
            messages (AsyncIterator[T]): An asynchronous iterator of messages.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            headers (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.

        Returns:
            None

        """
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
        self._headers = headers if headers is not None else Headers()
        self._method = method if method else HTTPMethod.POST.value

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

    @method.setter
    def method(self, value: str) -> None:
        """Set the request method."""
        self._method = value


class StreamRequest[T](RequestCommon):
    """StreamRequest class represents a request that can handle streaming messages.

    Attributes:
        messages (AsyncIterator[T]): An asynchronous iterator of messages.
        _spec (Spec): The specification for the request.
        _peer (Peer): The peer information.
        _headers (Headers): The request headers.
        _method (str): The HTTP method used for the request.

    """

    _messages: AsyncIterator[T]
    timeout: float | None

    def __init__(
        self,
        messages: AsyncIterator[T] | T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        timeout: float | None = None,
    ) -> None:
        """Initialize a new Request instance.

        Args:
            messages (AsyncIterator[T]): An asynchronous iterator of messages.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            headers (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.

        Returns:
            None

        """
        super().__init__(spec, peer, headers, method)
        self._messages = messages if isinstance(messages, AsyncIterator) else aiterate([messages])
        self.timeout = timeout

    @property
    def messages(self) -> AsyncIterator[T]:
        """Return the request message."""
        return self._messages


class UnaryRequest[T](RequestCommon):
    """UnaryRequest is a class that encapsulates a request with a message, specification, peer, headers, and method.

    Attributes:
        message (Req): The request message.
        _spec (Spec): The specification of the request.
        _peer (Peer): The peer associated with the request.
        _headers (Mapping[str, str]): The headers of the request.
        _method (str): The method of the request.

    """

    _message: T
    timeout: float | None

    def __init__(
        self,
        message: T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        timeout: float | None = None,
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
        super().__init__(spec, peer, headers, method)
        self._message = message
        self.timeout = timeout

    @property
    def message(self) -> T:
        """Return the request message."""
        return self._message


class ResponseCommon:
    """ResponseCommon is a class that encapsulates common response attributes such as headers and trailers.

    Attributes:
        _headers (Headers): The headers of the response.
        _trailers (Headers): The trailers of the response.

    """

    _headers: Headers
    _trailers: Headers

    def __init__(
        self,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initialize the response with a message."""
        self._headers = headers if headers is not None else Headers()
        self._trailers = trailers if trailers is not None else Headers()

    @property
    def headers(self) -> Headers:
        """Return the response headers."""
        return self._headers

    @property
    def trailers(self) -> Headers:
        """Return the response trailers."""
        return self._trailers


class UnaryResponse[T](ResponseCommon):
    """Response class for handling responses."""

    _message: T

    def __init__(
        self,
        message: T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initialize the response with a message."""
        super().__init__(headers, trailers)
        self._message = message

    @property
    def message(self) -> T:
        """Return the response message."""
        return self._message


class StreamResponse[T](ResponseCommon):
    """Response class for handling responses."""

    _messages: AsyncIterator[T]

    def __init__(
        self,
        messages: AsyncIterator[T] | T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initialize the response with a message."""
        super().__init__(headers, trailers)
        self._messages = messages if isinstance(messages, AsyncIterator) else aiterate([messages])

    @property
    def messages(self) -> AsyncIterator[T]:
        """Return the response message."""
        return self._messages


class UnaryHandlerConn(abc.ABC):
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
    async def send(self, message: Any) -> None:
        """Send a message.

        This method should be implemented by subclasses to define how the message
        should be sent.

        Args:
            message (Any): The message to be sent.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

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

    @abc.abstractmethod
    async def send_error(self, error: ConnectError) -> None:
        """Send an error message.

        This method should be implemented to handle the sending of error messages
        in a specific manner defined by the subclass.

        Args:
            error (ConnectError): The error to be sent.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError()


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
    def receive(self, message: Any) -> AsyncIterator[Any]:
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
    async def send(self, messages: AsyncIterator[Any]) -> None:
        """Send a stream of messages asynchronously.

        Args:
            messages (AsyncIterator[Any]): An asynchronous iterator that yields messages to be sent.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

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

    @abc.abstractmethod
    async def send_error(self, error: ConnectError) -> None:
        """Send an error message.

        This method should be implemented to handle the process of sending an error message
        when a ConnectError occurs.

        Args:
            error (ConnectError): The error that needs to be sent.

        Raises:
            NotImplementedError: This method is not yet implemented.

        """
        raise NotImplementedError()


class UnaryClientConn:
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
    async def send(self, message: Any, timeout: float | None) -> bytes:
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


class StreamingClientConn:
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
    async def send(self, messages: AsyncIterator[Any], timeout: float | None) -> None:
        """Send a stream of messages."""
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


async def receive_unary_request[T](conn: UnaryHandlerConn, t: type[T]) -> UnaryRequest[T]:
    """Receives a unary request from the given connection and returns a UnaryRequest object.

    Args:
        conn (UnaryHandlerConn): The connection from which to receive the unary request.
        t (type[T]): The type of the message to be received.

    Returns:
        UnaryRequest[T]: A UnaryRequest object containing the received message.

    """
    message = await receive_unary_message(conn, t)

    method = HTTPMethod.POST
    get_http_method = get_callable_attribute(conn, "get_http_method")
    if get_http_method:
        method = cast(HTTPMethod, get_http_method())

    return UnaryRequest(
        message=message,
        spec=conn.spec,
        peer=conn.peer,
        headers=conn.request_headers,
        method=method.value,
    )


async def receive_stream_request[T](conn: StreamingHandlerConn, t: type[T]) -> StreamRequest[T]:
    """Receive a stream request and returns a StreamRequest object.

    Args:
        conn (StreamingHandlerConn): The connection handler for the streaming request.
        t (type[T]): The type of the messages expected in the stream.

    Returns:
        StreamRequest[T]: An object containing the stream messages, connection specifications,
                          peer information, request headers, and HTTP method.

    """
    return StreamRequest(
        messages=receive_stream_message(conn, t),
        spec=conn.spec,
        peer=conn.peer,
        headers=conn.request_headers,
        method=HTTPMethod.POST,
    )


async def receive_stream_message[T](conn: StreamingHandlerConn, t: type[T]) -> AsyncIterator[T]:
    """Asynchronously receives and yields messages from a streaming connection.

    This function listens to a streaming connection and yields messages of the specified type.

    Args:
        conn (StreamingHandlerConn): The streaming connection handler.
        t (type[T]): The type of messages to receive.

    Yields:
        AsyncIterator[T]: An asynchronous iterator of messages of type T.

    """
    async for message in conn.receive(t):
        yield message


async def recieve_unary_response[T](conn: UnaryClientConn, t: type[T]) -> UnaryResponse[T]:
    """Receive a unary response from a streaming client connection.

    Args:
        conn (StreamingClientConn): The streaming client connection.
        t (type[T]): The type of the expected response message.

    Returns:
        UnaryResponse[T]: The response containing the message, response headers, and response trailers.

    """
    message = await receive_unary_message(conn, t)

    return UnaryResponse(message, conn.response_headers, conn.response_trailers)


async def recieve_stream_response[T](conn: StreamingClientConn, t: type[T], spec: Spec) -> StreamResponse[T]:
    """Receive a stream response from a streaming client connection.

    Args:
        conn (StreamingClientConn): The streaming client connection.
        t (type[T]): The type of the response to be received.
        spec (Spec): The specification for the request.

    Returns:
        StreamResponse[T]: The stream response containing the received data, response headers, and response trailers.

    """
    if spec.stream_type == StreamType.ClientStream:

        async def iterator() -> AsyncIterator[T]:
            count = 0
            async for message in conn.receive(t):
                yield message
                count += 1

            if count > 1:
                raise ConnectError(
                    "ClientStream should only receive one message, but received multiple.", Code.UNIMPLEMENTED
                )

            if count == 0:
                raise ConnectError("ClientStream should receive one message, but received none.", Code.UNIMPLEMENTED)

        return StreamResponse(iterator(), conn.response_headers, conn.response_trailers)
    else:
        return StreamResponse(conn.receive(t), conn.response_headers, conn.response_trailers)


async def receive_unary_message[T](conn: ReceiveConn, t: type[T]) -> T:
    """Receive a unary message from the given connection.

    Args:
        conn (ReceiveConn): The connection object to receive the message from.
        t (type[T]): The type of the message to be received.

    Returns:
        T: The received message of type T.

    """
    message = await conn.receive(t)
    return message
