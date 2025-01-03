"""Defines the streaming handler connection interfaces and related utilities."""

import abc
from enum import Enum
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel
from starlette.datastructures import MutableHeaders

from connect.request import ConnectRequest
from connect.response import Response


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

    def close(self, data: bytes) -> Response:
        """Close the connection with the provided data.

        Args:
            data (bytes): The data to be sent when closing the connection.

        Returns:
            Response: The response received after closing the connection.

        Raises:
            NotImplementedError: This method is not yet implemented.

        """
        raise NotImplementedError()

    def close_with_error(self, error: Exception) -> Response:
        """Close the connection and returns a response indicating an error.

        Args:
            error (Exception): The exception that caused the connection to close.

        Returns:
            Response: A response object indicating the error.

        Raises:
            NotImplementedError: This method is not yet implemented.

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
    return ConnectRequest(message)


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
