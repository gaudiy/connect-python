"""Defines the streaming handler connection interfaces and related utilities."""

import abc
import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from enum import Enum
from http import HTTPMethod
from typing import Any, cast

from pydantic import BaseModel

from connect.code import Code
from connect.error import ConnectError
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.utils import AsyncDataStream, aiterate, get_acallable_attribute, get_callable_attribute


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
        messages (AsyncIterable[T]): An asynchronous iterable of messages.
        _spec (Spec): The specification for the request.
        _peer (Peer): The peer information.
        _headers (Headers): The request headers.
        _method (str): The HTTP method used for the request.

    """

    _messages: AsyncIterable[T]
    timeout: float | None
    abort_event: asyncio.Event | None = None

    def __init__(
        self,
        messages: AsyncIterable[T] | T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        timeout: float | None = None,
        abort_event: asyncio.Event | None = None,
    ) -> None:
        """Initialize a new Request instance.

        Args:
            messages (AsyncIterable[T] | T): The request messages.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            headers (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.
            timeout (float): The timeout for the request.
            abort_event (asyncio.Event): An event to signal request abortion.

        Returns:
            None

        """
        super().__init__(spec, peer, headers, method)
        self._messages = messages if isinstance(messages, AsyncIterable) else aiterate([messages])
        self.timeout = timeout
        self.abort_event = abort_event

    @property
    def messages(self) -> AsyncIterable[T]:
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
    abort_event: asyncio.Event | None = None

    def __init__(
        self,
        message: T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        timeout: float | None = None,
        abort_event: asyncio.Event | None = None,
    ) -> None:
        """Initialize a new Request instance.

        Args:
            message (Req): The request message.
            spec (Spec): The specification for the request.
            peer (Peer): The peer information.
            headers (Mapping[str, str]): The request headers.
            method (str): The HTTP method used for the request.
            timeout (float): The timeout for the request.
            abort_event (asyncio.Event): An event to signal request abortion.

        Returns:
            None

        """
        super().__init__(spec, peer, headers, method)
        self._message = message
        self.timeout = timeout
        self.abort_event = abort_event

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

    _messages: AsyncIterable[T]

    def __init__(
        self,
        messages: AsyncIterable[T] | T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initialize the response with a message."""
        super().__init__(headers, trailers)
        self._messages = messages if isinstance(messages, AsyncIterable) else aiterate([messages])

    @property
    def messages(self) -> AsyncIterable[T]:
        """Return the response message."""
        return self._messages

    async def aclose(self) -> None:
        """Asynchronously close the response stream."""
        aclose = get_acallable_attribute(self._messages, "aclose")
        if aclose:
            await aclose()


class AsyncContentStream[T](AsyncIterable[T]):
    """AsyncContentStream is a generic asynchronous stream wrapper for async iterables, providing validation and iteration utilities based on stream type.

    Type Parameters:
        T: The type of elements yielded by the asynchronous iterable.

        iterable (AsyncIterable[T]): The asynchronous iterable to wrap.
        stream_type (StreamType): The type of stream (e.g., Unary, ServerStream) that determines validation behavior.

    Attributes:
        _iterable (AsyncIterable[T]): The underlying asynchronous iterable.
        stream_type (StreamType): The type of stream this instance represents.

    """

    def __init__(self, iterable: AsyncIterable[T], stream_type: StreamType) -> None:
        """Initialize a stream wrapper for an async iterable.

        This constructor stores the provided async iterable and its corresponding
        stream type for later processing.

        Args:
            iterable: An asynchronous iterable containing elements of type T.
            stream_type: The type of stream this iterable represents.

        Returns:
            None

        """
        self._iterable = iterable
        self.stream_type = stream_type

    async def __aiter__(self) -> AsyncIterator[T]:
        """Asynchronously iterates over the underlying iterable.

        If single message validation is required, wraps the iterable with a validation step.
        Otherwise, yields items directly from the iterable.

        Yields:
            T: Items from the underlying asynchronous iterable.

        """
        if self.stream_type == StreamType.Unary or self.stream_type == StreamType.ServerStream:
            item = await ensure_single(self._iterable)
            yield item
        else:
            async for item in self._iterable:
                yield item


async def ensure_single[T](iterable: AsyncIterable[T], aclose: Callable[[], Awaitable[None]] | None = None) -> T:
    """Asynchronously ensures that the given async iterable yields exactly one item.

    Iterates over the provided async iterable (after validating its content stream)
    and returns the single item if present. Raises a ConnectError if the iterable
    is empty or contains more than one item. Optionally closes the iterable by calling
    the provided aclose function after processing.

    Args:
        iterable (AsyncIterable[T]): An asynchronous iterable expected to yield exactly one item.
        aclose (Callable[[], Awaitable[None]] | None, optional): A callable that asynchronously
            closes the stream when invoked. If provided, will be called in a finally block.

    Returns:
        T: The single item yielded by the iterable.

    Raises:
        ConnectError: If the iterable yields no items or more than one item.

    """
    try:
        iterator = iterable.__aiter__()
        try:
            first = await iterator.__anext__()
            try:
                await iterator.__anext__()
                raise ConnectError("protocol error: expected only one message, but got multiple", Code.UNIMPLEMENTED)
            except StopAsyncIteration:
                return first
        except StopAsyncIteration:
            raise ConnectError("protocol error: expected one message, but got none", Code.UNIMPLEMENTED) from None
    finally:
        if aclose:
            await aclose()


class StreamingHandlerConn(abc.ABC):
    """Abstract base class for a streaming handler connection.

    This class defines the interface for handling streaming connections, including
    methods for specifying the connection, handling peer communication, receiving
    and sending messages, and managing request and response headers and trailers.

    """

    @abc.abstractmethod
    def parse_timeout(self) -> float | None:
        """Parse the timeout value."""
        raise NotImplementedError()

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
    def receive(self, message: Any) -> AsyncContentStream[Any]:
        """Receives a message and returns an asynchronous content stream.

        Args:
            message (Any): The message to be processed.

        Returns:
            AsyncContentStream[Any]: An asynchronous stream of content resulting from processing the message.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Generate and return the request headers.

        Returns:
            Headers: The request headers.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Send a stream of messages asynchronously.

        Args:
            messages (AsyncIterable[Any]): The messages to be sent.
                                         For unary operations, this should be an iterable with a single item.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Retrieve the response headers.

        Returns:
            Headers: The response headers.

        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Headers: The response trailers.

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
    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and processes it."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Return the request headers."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Any, timeout: float | None, abort_event: asyncio.Event | None) -> bytes:
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

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Asynchronously close the connection."""
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
    def receive(self, message: Any, abort_event: asyncio.Event | None) -> AsyncIterator[Any]:
        """Receives a message and processes it."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Return the request headers."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(
        self, messages: AsyncIterable[Any], timeout: float | None, abort_event: asyncio.Event | None
    ) -> None:
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

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Asynchronously close the connection."""
        raise NotImplementedError()


async def receive_unary_request[T](conn: StreamingHandlerConn, t: type[T]) -> UnaryRequest[T]:
    """Receives a unary request from the given connection and returns a UnaryRequest object.

    Args:
        conn (StreamingHandlerConn): The connection from which to receive the unary request.
        t (type[T]): The type of the message to be received.

    Returns:
        UnaryRequest[T]: A UnaryRequest object containing the received message.

    """
    stream = conn.receive(t)
    message = await ensure_single(stream)

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
    stream = conn.receive(t)

    return StreamRequest(
        messages=stream,
        spec=conn.spec,
        peer=conn.peer,
        headers=conn.request_headers,
        method=HTTPMethod.POST,
    )


async def recieve_unary_response[T](
    conn: StreamingClientConn, t: type[T], abort_event: asyncio.Event | None
) -> UnaryResponse[T]:
    """Receives a unary response message from a streaming client connection.

    This asynchronous function waits for a unary message of the specified type from the given
    streaming client connection. It also handles optional abortion via an asyncio event.
    The response, along with any headers and trailers from the connection, is wrapped in a
    UnaryResponse object and returned.

    Args:
        conn (StreamingClientConn): The streaming client connection to receive the message from.
        t (type[T]): The expected type of the message to be received.
        abort_event (asyncio.Event | None): Optional event to signal abortion of the receive operation.

    Returns:
        UnaryResponse[T]: The received message and associated response metadata.

    Raises:
        Any exceptions raised by `receive_unary_message` or connection errors.

    """
    message = await receive_unary_message(conn, t, abort_event)

    return UnaryResponse(message, conn.response_headers, conn.response_trailers)


async def recieve_stream_response[T](
    conn: StreamingClientConn, t: type[T], spec: Spec, abort_event: asyncio.Event | None
) -> StreamResponse[T]:
    """Handle receiving a stream response from a streaming client connection.

    Args:
        conn (StreamingClientConn): The streaming client connection used to receive the stream.
        t (type[T]): The type of the messages expected in the stream.
        spec (Spec): The specification of the stream, including its type.
        abort_event (asyncio.Event | None): An optional event to signal abortion of the stream.

    Returns:
        StreamResponse[T]: A response object containing the received stream, response headers,
        and response trailers.

    Raises:
        Any exceptions raised during the reception of the stream or processing of the messages.

    Notes:
        - If the stream type is `StreamType.ClientStream`, it expects exactly one message
          and wraps it in a single-message stream.
        - For other stream types, it directly returns the received stream.

    """
    receive_stream = AsyncDataStream[T](conn.receive(t, abort_event), conn.aclose)

    if spec.stream_type == StreamType.ClientStream:
        single_message = await ensure_single(receive_stream, receive_stream.aclose)

        return StreamResponse(
            AsyncDataStream[T](aiterate([single_message])), conn.response_headers, conn.response_trailers
        )
    else:
        return StreamResponse(receive_stream, conn.response_headers, conn.response_trailers)


async def receive_unary_message[T](conn: StreamingClientConn, t: type[T], abort_event: asyncio.Event | None) -> T:
    """Receives exactly one unary message of the specified type from a streaming connection.

    Args:
        conn (StreamingClientConn): The streaming client connection to receive the message from.
        t (type[T]): The expected type of the message to receive.
        abort_event (asyncio.Event | None): An optional event to signal abortion of the receive operation.

    Returns:
        T: The single message received of type `t`.

    Raises:
        ConnectError: If zero or more than one message is received, or if the receive operation fails.

    """
    return await ensure_single(conn.receive(t, abort_event), conn.aclose)
