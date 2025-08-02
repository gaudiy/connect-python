# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Core components and abstractions for the Connect protocol."""

import abc
import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from enum import Enum
from http import HTTPMethod
from typing import Any, cast

from pydantic import BaseModel

from connect.code import Code
from connect.content_stream import AsyncDataStream
from connect.error import ConnectError
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.utils import aiterate, get_acallable_attribute, get_callable_attribute


class StreamType(Enum):
    """Enumeration of the different types of RPC streams.

    This enum categorizes the communication patterns between a client and a server
    for a specific RPC method, mirroring the concepts found in frameworks like gRPC.

    Attributes:
        Unary: A simple RPC where the client sends a single request and receives a
               single response.
        ClientStream: An RPC where the client sends a stream of messages and the
                      server sends back a single response.
        ServerStream: An RPC where the client sends a single request and receives a
                      stream of messages in response.
        BiDiStream: An RPC where both the client and the server send a stream of
                    messages to each other.
    """

    Unary = "Unary"
    ClientStream = "ClientStream"
    ServerStream = "ServerStream"
    BiDiStream = "BiDiStream"


class Spec(BaseModel):
    """Defines the specification for a remote procedure call.

    Args:
        procedure: The fully qualified name of the procedure to be called.
        descriptor: The descriptor for the procedure, which may contain schema or other metadata.
        stream_type: The streaming behavior of the procedure.
        idempotency_level: The idempotency level, which determines how the procedure handles retries.
    """

    procedure: str
    descriptor: Any
    stream_type: StreamType
    idempotency_level: IdempotencyLevel


class Address(BaseModel):
    """Represents a network address, consisting of a host and a port.

    Attributes:
        host (str): The hostname or IP address.
        port (int): The port number.
    """

    host: str
    port: int


class Peer(BaseModel):
    """Represents a peer in the network.

    Attributes:
        address: The network address of the peer.
        protocol: The communication protocol used by the peer (e.g., 'http', 'ws').
        query: A mapping of query parameters for the peer connection.
    """

    address: Address | None
    protocol: str
    query: Mapping[str, str]


class RequestCommon:
    """Represents the common context for a Connect RPC request or response.

    This class encapsulates information that is shared between requests and responses
    in the Connect protocol, such as the RPC specification, peer details, HTTP
    headers, and the HTTP method used.

    Attributes:
        spec (Spec): The RPC specification, including procedure name, stream type,
            and idempotency level.
        peer (Peer): Information about the network peer, such as address and protocol.
        headers (Headers): The HTTP headers associated with the request or response.
        method (str): The HTTP method used for the request (e.g., 'POST').
    """

    _spec: Spec
    _peer: Peer
    _headers: Headers
    _method: str
    matadata: dict[str, Any] | None = None

    def __init__(
        self,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        matadata: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the RPC context.

        Args:
            spec (Spec | None, optional): The specification for the RPC.
                If None, a default Spec is created. Defaults to None.
            peer (Peer | None, optional): Information about the network peer.
                If None, a default Peer is created. Defaults to None.
            headers (Headers | None, optional): The request headers.
                If None, an empty Headers object is created. Defaults to None.
            method (str | None, optional): The HTTP method of the request.
                Defaults to POST.
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
        self.matadata = matadata if matadata is not None else {}

    @property
    def spec(self) -> Spec:
        """Gets the service specification.

        Returns:
            Spec: The service specification object.
        """
        return self._spec

    @spec.setter
    def spec(self, value: Spec) -> None:
        """Sets the specification for the Connect instance.

        Args:
            value: The specification object.
        """
        self._spec = value

    @property
    def peer(self) -> Peer:
        """Gets the peer object for this connection.

        Returns:
            Peer: The peer object.
        """
        return self._peer

    @peer.setter
    def peer(self, value: Peer) -> None:
        """Sets the peer for the connection.

        Args:
            value: The Peer instance to set.
        """
        self._peer = value

    @property
    def headers(self) -> Headers:
        """Gets the headers for the message.

        Returns:
            The headers for the message.
        """
        return self._headers

    @property
    def method(self) -> str:
        """Gets the method name.

        Returns:
            str: The name of the method.
        """
        return self._method

    @method.setter
    def method(self, value: str) -> None:
        """Sets the HTTP method for the request.

        Args:
            value: The HTTP method (e.g., "GET", "POST").
        """
        self._method = value


class StreamRequest[T](RequestCommon):
    """Represents a streaming request, containing an asynchronous iterable of messages.

    This class is used for RPCs where the client sends a stream of messages,
    such as client streaming or bidirectional streaming calls. It provides
    access to the messages as an async iterable and helper methods to consume them.

    Type Parameters:
        T: The type of the messages in the stream.

        content: The content to be processed, which can be a single item of type T
            or an async iterable of items.

    Attributes:
        messages (AsyncIterable[T]): The request messages as an async iterable.
        spec (Spec | None): The specification for the RPC.
        peer (Peer | None): Information about the remote peer.
        headers (Headers | None): The request headers.
        method (str | None): The HTTP method used for the request.
    """

    _messages: AsyncIterable[T]

    def __init__(
        self,
        content: AsyncIterable[T] | T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initializes a new request.

        Args:
            content: The main content of the request. Can be a single message
            or an asynchronous iterable of messages.
            spec: The specification for the request. Defaults to None.
            peer: The peer that initiated the request. Defaults to None.
            headers: The headers associated with the request. Defaults to None.
            method: The method name for the request. Defaults to None.
        """
        super().__init__(
            spec,
            peer,
            headers,
            method,
            metadata,
        )
        self._messages = content if isinstance(content, AsyncIterable) else aiterate([content])

    @property
    def messages(self) -> AsyncIterable[T]:
        """An asynchronous iterator over received messages.

        This allows you to iterate through messages from the server as they arrive
        using an ``async for`` loop.

        Yields:
            The next available message from the connection.
        """
        return self._messages

    async def single(self) -> T:
        """Asynchronously waits for and returns the single expected message.

        This method is used when exactly one message is expected from the
        underlying asynchronous message source.

        Returns:
            T: The one and only message received.

        Raises:
            ValueError: If the number of messages received is not equal to one
                (i.e., zero or more than one).
        """
        return await ensure_single(self._messages)


class UnaryRequest[T](RequestCommon):
    """Represents a unary (non-streaming) request.

    This class encapsulates a single request message along with its associated
    metadata, such as headers and peer information. It is used for interactions
    where a single request message is sent and a single response is expected.

    Type Parameters:
        T: The type of the request message/content.

    Attributes:
        message (T): The request message or payload.
        spec (Spec | None): Specification object defining behavior or configuration.
        peer (Peer | None): Peer object representing the remote endpoint.
        headers (Headers | None): Metadata associated with the request.
        method (str | None): The RPC method being called.
    """

    _message: T

    def __init__(
        self,
        content: T,
        spec: Spec | None = None,
        peer: Peer | None = None,
        headers: Headers | None = None,
        method: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initializes the request object.

        Args:
            content (T): The content of the message.
            spec (Spec | None, optional): The request specification. Defaults to None.
            peer (Peer | None, optional): Information about the peer. Defaults to None.
            headers (Headers | None, optional): The request headers. Defaults to None.
            method (str | None, optional): The request method. Defaults to None.
        """
        super().__init__(spec, peer, headers, method, metadata)
        self._message = content

    @property
    def message(self) -> T:
        """Get the underlying message.

        Returns:
            The message object.
        """
        return self._message


class ResponseCommon:
    """A base class representing common properties for all Connect response types.

    This class encapsulates the headers and trailers that are common to both
    unary and streaming responses.

    Attributes:
        headers (Headers): The response headers.
        trailers (Headers): The response trailers.
    """

    _headers: Headers
    _trailers: Headers

    def __init__(
        self,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initializes the instance.

        Args:
            headers: Optional initial headers.
            trailers: Optional initial trailers.
        """
        self._headers = headers if headers is not None else Headers()
        self._trailers = trailers if trailers is not None else Headers()

    @property
    def headers(self) -> Headers:
        """Returns the headers for the request.

        Returns:
            Headers: The headers for the request.
        """
        return self._headers

    @property
    def trailers(self) -> Headers:
        """Returns the trailers of the response.

        Trailers are headers sent after the message body. They are only available
        after the entire response body has been read.

        Returns:
            Headers: The trailers. An empty Headers object if no trailers were sent.
        """
        return self._trailers


class UnaryResponse[T](ResponseCommon):
    """Represents a unary response from a Connect RPC.

    This class encapsulates a single response message, along with its
    associated headers and trailers.

    Args:
        content (T): The deserialized response message.
        headers (Headers | None): The response headers.
        trailers (Headers | None): The response trailers.

    Attributes:
        message (T): The deserialized response message.
        headers (Headers): The response headers.
        trailers (Headers): The response trailers.
    """

    _message: T

    def __init__(
        self,
        content: T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initializes the message object.

        Args:
            content: The message content.
            headers: Optional initial headers.
            trailers: Optional initial trailers.
        """
        super().__init__(headers, trailers)
        self._message = content

    @property
    def message(self) -> T:
        """Returns the message associated with the response.

        Returns:
            The message of type T.
        """
        return self._message


class StreamResponse[T](ResponseCommon):
    """Represents a streaming response from a Connect RPC.

    This class encapsulates the response headers, trailers, and the asynchronous
    stream of response messages. It is used for server-streaming and
    bidirectional-streaming RPCs where the server sends multiple messages over time.

    The primary way to interact with a `StreamResponse` is to iterate over its
    `messages` property to consume the stream of incoming data. For RPCs that are
    expected to return exactly one message in the stream (like client-streaming),
    the `single()` method can be used for convenience.

    Type Parameters:
        T: The type of the messages in the response stream.

    Attributes:
        headers (Headers | None): The response headers.
        trailers (Headers | None): The response trailers.
    """

    _messages: AsyncIterable[T]

    def __init__(
        self,
        content: AsyncIterable[T] | T,
        headers: Headers | None = None,
        trailers: Headers | None = None,
    ) -> None:
        """Initializes the request.

        Args:
            content: The content of the request. Can be a single message or an async iterable of messages.
            headers: The headers of the request.
            trailers: The trailers of the request.
        """
        super().__init__(headers, trailers)
        self._messages = content if isinstance(content, AsyncIterable) else aiterate([content])

    @property
    def messages(self) -> AsyncIterable[T]:
        """An asynchronous iterator over the messages received from the server.

        This method provides a way to consume messages from the server as they
        arrive. It is intended to be used with an `async for` loop.

        Yields:
            T: The next message received from the server.
        """
        return self._messages

    async def single(self) -> T:
        """Asynchronously gets the single message from the stream.

        This method consumes the underlying asynchronous message stream and
        ensures that it contains exactly one message.

        Returns:
            The single message from the stream.

        Raises:
            ValueError: If the stream is empty or contains more than one message.
        """
        return await ensure_single(self._messages)

    async def aclose(self) -> None:
        """Asynchronously close the response stream."""
        aclose = get_acallable_attribute(self._messages, "aclose")
        if aclose:
            await aclose()


async def ensure_single[T](iterable: AsyncIterable[T], aclose: Callable[[], Awaitable[None]] | None = None) -> T:
    """Ensures an async iterable yields exactly one item and returns it.

    This is a helper function for handling unary responses in a streaming context.
    It consumes the iterable to verify its cardinality.

    Args:
        iterable: The asynchronous iterable to consume.
        aclose: An optional awaitable callable to be executed for cleanup
            in a finally block.

    Returns:
        The single item from the iterable.

    Raises:
        ConnectError: If the iterable contains zero or more than one item.
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
    """Abstract base class for handling streaming connections.

    This class defines the interface for a streaming handler, which is responsible
    for managing the lifecycle of a streaming request and response. It includes
    methods for sending and receiving data streams, accessing request and response
    metadata (headers and trailers), and handling errors.

    Concrete implementations must provide logic for all abstract methods and
    properties defined in this class to facilitate a specific communication
    protocol or transport layer.
    """

    @abc.abstractmethod
    def parse_timeout(self) -> float | None:
        """Abstract method to parse the timeout from the configuration.

        Subclasses must implement this method to extract the timeout value
        from their specific configuration source.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            The request timeout in seconds as a float, or None if no timeout
            is configured.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Returns the specification of the connector.

        This is an abstract method that must be implemented by subclasses.
        It should return a `Spec` object that defines the connector's
        metadata, capabilities, and configuration schema.

        Returns:
            Spec: An object containing the connector's specification.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Gets the peer of the connection.

        This is an abstract method that must be implemented by subclasses.

        Raises:
            NotImplementedError: This method is not implemented.

        Returns:
            Peer: An object representing the connected peer.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receive messages.

        This method is intended to be implemented by subclasses to handle the
        reception of a stream of messages. It should be an asynchronous generator
        that yields messages as they are received.

        Args:
            message: The initial message or subscription request that triggers
                the stream of incoming messages.

        Yields:
            Messages of any type received from the source.

        Raises:
            NotImplementedError: This base method is not implemented and must
                be overridden in a subclass.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Abstract method to get request headers.

        Subclasses must implement this method to provide the necessary headers
        for making API requests. This typically includes headers for
        authentication, content type, etc.

        Raises:
            NotImplementedError: If the method is not overridden in a subclass.

        Returns:
            Headers: A dictionary-like object representing the HTTP headers.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Asynchronously sends a stream of messages.

        This method takes an asynchronous iterable of messages and sends them
        over the connection.

        Args:
            messages: An asynchronous iterable yielding messages to be sent.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Gets the response headers.

        This is an abstract method that must be implemented by subclasses.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            Headers: A dictionary-like object containing the response headers.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Returns the response trailers.

        This method is called after the response body has been fully read.
        It provides access to any trailing headers sent by the server.

        Raises:
            NotImplementedError: This method is not implemented in the base class
                and must be overridden in a subclass.

        Returns:
            Headers: A Headers object containing the trailing headers of the response.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def send_error(self, error: ConnectError) -> None:
        """Sends a ConnectError to the client.

        This is an abstract method that must be implemented by a subclass.
        It is responsible for serializing the error and sending it over the
        transport layer.

        Args:
            error: The ConnectError instance to send.

        Raises:
            NotImplementedError: This method must be overridden by a subclass.
        """
        raise NotImplementedError()


class UnaryClientConn(abc.ABC):
    """Abstract base class defining the interface for a unary client connection.

    This class outlines the contract for managing a single request-response
    interaction with a server. Implementations of this class are responsible for
    handling the specifics of the communication protocol.

    Attributes:
        spec (Spec): The specification details for the RPC call.
        peer (Peer): Information about the remote peer (server).
        request_headers (Headers): The headers for the outgoing request.
        response_headers (Headers): The headers from the server's response.
        response_trailers (Headers): The trailers from the server's response.
    """

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Returns the service specification.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            Spec: The specification for the service.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Returns the peer of the connection.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            Peer: The `Peer` instance representing the other side of the connection.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a stream of messages in response to an initial message.

        This method is an asynchronous generator that sends an initial message
        and then yields incoming messages as they are received.

        Args:
            message: The initial message to send to initiate the stream.

        Yields:
            An asynchronous iterator that provides messages as they are received.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Get the headers for an API request.

        This is an abstract method that must be implemented by subclasses.
        It is responsible for constructing and returning the headers required
        for making requests, which may include authentication tokens.

        Raises:
            NotImplementedError: This method must be overridden in a subclass.

        Returns:
            Headers: A dictionary-like object containing the request headers.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Any, timeout: float | None, abort_event: asyncio.Event | None) -> bytes:
        """Sends a message and waits for a response.

        This is an abstract method that must be implemented by a subclass.

        Args:
            message: The message payload to send.
            timeout: The maximum time in seconds to wait for a response.
                If None, the call will wait indefinitely.
            abort_event: An optional asyncio event that can be set to
                prematurely abort the send operation.

        Returns:
            The raw response received as bytes.

        Raises:
            NotImplementedError: This is an abstract method.
            asyncio.TimeoutError: If the timeout is reached before a response is received.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Get the response headers.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            An object representing the response headers.

        Raises:
            NotImplementedError: This method is not implemented in the base class.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Returns the response trailers.

        This method is called after the response body has been fully read.
        It will not be called if the server does not send trailers.

        Raises:
            NotImplementedError: This method is not implemented.

        Returns:
            Headers: The response trailers.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def on_request_send(self, fn: Callable[..., Any]) -> None:
        """Registers a callback function to be executed before a request is sent.

        This method is intended to be used as a decorator. The decorated function
        will be called with the request details, allowing for inspection or
        modification of the request just before it is dispatched.

        Args:
            fn: The callback function to execute when a request is about to be sent.
                The arguments passed to this function will depend on the specific
                implementation.

        Raises:
            NotImplementedError: This method is not yet implemented and must be
                overridden in a subclass.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Asynchronously close the connection.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError()


class StreamingClientConn(abc.ABC):
    """Abstract base class defining the interface for a streaming client connection.

    This class outlines the contract that all concrete streaming client connection
    implementations must adhere to. It provides a standardized way to handle
    bidirectional streaming communication, including sending and receiving data streams,
    accessing headers and trailers, and managing the connection lifecycle.

    Attributes:
        spec (Spec): The specification details for the connection.
        peer (Peer): Information about the connected peer.
        request_headers (Headers): The headers for the outgoing request.
        response_headers (Headers): The headers from the incoming response.
        response_trailers (Headers): The trailers from the incoming response.
    """

    @property
    @abc.abstractmethod
    def spec(self) -> Spec:
        """Returns the component specification.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            Spec: The component specification object.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Gets the peer for this connection.

        A peer represents the remote endpoint of the connection.

        Returns:
            Peer: An object representing the connected peer.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(
        self, message: Any, abort_event: asyncio.Event | None, metadata: dict[str, Any] | None = None
    ) -> AsyncIterator[Any]:
        """Asynchronously receives a stream of messages.

        This method sends an initial message and then listens for a stream of
        responses. It is an async generator that yields messages as they arrive.

        Args:
            message (Any): The initial message to send to start the stream.
            abort_event (asyncio.Event | None): An optional event that can be set
            to signal the termination of the receive operation.

        Yields:
            Any: Messages received from the stream.

        Raises:
            NotImplementedError: This method must be implemented by a subclass.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def request_headers(self) -> Headers:
        """Abstract method to get the request headers.

        This method should be implemented by subclasses to provide the
        necessary headers for making requests.

        Raises:
            NotImplementedError: This is an abstract method that must be
                implemented by a subclass.

        Returns:
            Headers: A dictionary-like object containing the HTTP headers.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(
        self,
        messages: AsyncIterable[Any],
        timeout: float | None,
        abort_event: asyncio.Event | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Asynchronously sends a stream of messages.

        This is an abstract method that must be implemented by a subclass. It is
        designed to handle sending an asynchronous stream of messages, with support
        for timeouts and external cancellation.

        Args:
            messages: An asynchronous iterable of messages to send.
            timeout: The maximum time in seconds to wait for the send operation
                to complete. If None, there is no timeout.
            abort_event: An asyncio.Event that, if set, will signal the
                operation to abort.

        Raises:
            NotImplementedError: This method must be implemented by a subclass.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_headers(self) -> Headers:
        """Gets the HTTP response headers.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            Headers: An object containing the response headers.
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def response_trailers(self) -> Headers:
        """Get the response trailers.

        This should only be called after the response body has been fully read.
        Not all responses will have trailers.

        Returns:
            Headers: A collection of the response trailer headers.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def on_request_send(self, fn: Callable[..., Any]) -> None:
        """Registers a callback function to be executed before a request is sent.

        This method is intended to be used as a decorator. The decorated function
        will be called with the request object as its argument before the request
        is sent. This allows for last-minute modifications, logging, or other
        pre-request processing.

        Example:
            @client.on_request_send
            def add_custom_header(request):
                request.headers['X-Custom-Header'] = 'my-value'

        Args:
            fn (Callable[..., Any]): The callback function to be executed. It will
                receive the request object as its argument.

        Raises:
            NotImplementedError: This method is not implemented and should be
                overridden in a subclass.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Asynchronously close the connection and release all resources.

        This method is a coroutine.
        """
        raise NotImplementedError()


async def receive_unary_request[T](conn: StreamingHandlerConn, t: type[T]) -> UnaryRequest[T]:
    """Receives a single message from a streaming connection to form a unary request.

    This function reads from the provided connection's stream, ensuring that exactly
    one message is present. It then packages this message along with metadata from the
    connection (e.g., headers, peer, HTTP method) into a UnaryRequest object.

    Args:
        conn (StreamingHandlerConn): The streaming connection to receive from.
        t (type[T]): The type to which the incoming message should be deserialized.

    Returns:
        UnaryRequest[T]: An object representing the complete unary request, including
            the deserialized message and connection metadata.

    Raises:
        Exception: If the stream does not contain exactly one message.
    """
    stream = conn.receive(t)
    message = await ensure_single(stream)

    method = HTTPMethod.POST
    get_http_method = get_callable_attribute(conn, "get_http_method")
    if get_http_method:
        method = cast(HTTPMethod, get_http_method())

    return UnaryRequest(
        content=message,
        spec=conn.spec,
        peer=conn.peer,
        headers=conn.request_headers,
        method=method.value,
    )


async def receive_stream_request[T](conn: StreamingHandlerConn, t: type[T]) -> StreamRequest[T]:
    """Constructs a StreamRequest from an incoming streaming connection.

    This function adapts the raw message stream from a StreamingHandlerConn into a
    standardized StreamRequest object. It intelligently handles different stream types:
    - For Server Streams, it awaits and wraps a single incoming message into an
        async iterator.
    - For Client and Bidirectional Streams, it uses the incoming async iterator
        of messages directly.

    Generic Parameters:
            T: The data type of the message(s) in the stream.

    Args:
            conn: The active streaming connection handler from which to receive data.
            t: The expected type of the incoming message(s) for deserialization.

    Returns:
            A StreamRequest object containing the message content as an async
            iterator, along with connection metadata like headers and peer info.
    """
    if conn.spec.stream_type == StreamType.ServerStream:
        message = await ensure_single(conn.receive(t))

        return StreamRequest(
            content=aiterate([message]),
            spec=conn.spec,
            peer=conn.peer,
            headers=conn.request_headers,
            method=HTTPMethod.POST.value,
        )
    else:
        return StreamRequest(
            content=conn.receive(t),
            spec=conn.spec,
            peer=conn.peer,
            headers=conn.request_headers,
            method=HTTPMethod.POST.value,
        )


async def receive_unary_response[T](
    conn: StreamingClientConn, t: type[T], abort_event: asyncio.Event | None
) -> UnaryResponse[T]:
    """Receives a single message from a streaming connection for a unary-style RPC.

    This helper function waits for a single message from the given streaming
    connection, ensuring the stream closes after one message is received. It's
    intended for use with unary RPCs that are transported over a streaming protocol.

    Args:
        conn (StreamingClientConn): The streaming client connection to receive from.
        t (type[T]): The expected type of the response message for deserialization.
        abort_event (asyncio.Event | None): An optional event to signal cancellation
            of the receive operation.

    Returns:
        UnaryResponse[T]: A response object containing the single deserialized
            message, along with the response headers and trailers from the
            connection.

    Raises:
        Exception: If the stream is closed before a message is received, or if
            more than one message is received.
    """
    message = await ensure_single(conn.receive(t, abort_event), conn.aclose)

    return UnaryResponse(message, conn.response_headers, conn.response_trailers)


async def receive_stream_response[T](
    conn: StreamingClientConn,
    t: type[T],
    spec: Spec,
    abort_event: asyncio.Event | None,
    metadata: dict[str, Any] | None = None,
) -> StreamResponse[T]:
    """Receives a streaming response from the server.

    This function adapts the behavior based on the stream type defined in the
    specification. For client streams, it awaits a single response message. For
    server or bidirectional streams, it returns an async iterator for the
    incoming messages.

    Args:
        conn (StreamingClientConn): The streaming client connection.
        t (type[T]): The expected type of the response message(s).
        spec (Spec): The RPC method's specification.
        abort_event (asyncio.Event | None): An event to signal abortion of the receive operation.

    Returns:
        StreamResponse[T]: A stream response object containing the data stream,
            headers, and trailers.
    """
    if spec.stream_type == StreamType.ClientStream:
        single_message = await ensure_single(conn.receive(t, abort_event, metadata))

        return StreamResponse(
            AsyncDataStream[T](aiterate([single_message]), conn.aclose), conn.response_headers, conn.response_trailers
        )
    else:
        return StreamResponse(
            AsyncDataStream[T](conn.receive(t, abort_event, metadata), conn.aclose),
            conn.response_headers,
            conn.response_trailers,
        )
