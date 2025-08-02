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

"""Defines the abstract interfaces and helpers for RPC protocol implementations."""

import abc
from http import HTTPMethod

from pydantic import BaseModel, ConfigDict, Field
from yarl import URL

from gconnect.code import Code
from gconnect.codec import Codec, ReadOnlyCodecs
from gconnect.compression import COMPRESSION_IDENTITY, Compression
from gconnect.connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamingHandlerConn,
    StreamType,
)
from gconnect.connection_pool import AsyncConnectionPool
from gconnect.error import ConnectError
from gconnect.headers import Headers
from gconnect.idempotency_level import IdempotencyLevel
from gconnect.request import Request
from gconnect.response_writer import ServerResponseWriter

PROTOCOL_CONNECT = "connect"
PROTOCOL_GRPC = "grpc"
PROTOCOL_GRPC_WEB = "grpc-web"

HEADER_CONTENT_TYPE = "Content-Type"
HEADER_CONTENT_ENCODING = "Content-Encoding"
HEADER_CONTENT_LENGTH = "Content-Length"
HEADER_HOST = "Host"
HEADER_USER_AGENT = "User-Agent"
HEADER_TRAILER = "Trailer"
HEADER_DATE = "Date"


class ProtocolHandlerParams(BaseModel):
    """Parameters for configuring a protocol handler.

    This class encapsulates all the configuration options needed to set up
    a protocol handler for Connect RPC communication.

    Attributes:
        spec: The service specification defining available methods and types.
        codecs: Read-only collection of codecs for message serialization/deserialization.
        compressions: List of supported compression algorithms.
        compress_min_bytes: Minimum message size in bytes before compression is applied.
        read_max_bytes: Maximum number of bytes that can be read in a single operation.
        send_max_bytes: Maximum number of bytes that can be sent in a single operation.
        require_connect_protocol_header: Whether to require Connect protocol headers.
        idempotency_level: The level of idempotency support for operations.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    spec: Spec
    """The service specification defining available methods and types."""
    codecs: ReadOnlyCodecs
    """Read-only collection of codecs for message serialization/deserialization."""
    compressions: list[Compression]
    """List of supported compression algorithms."""
    compress_min_bytes: int
    """Minimum message size in bytes before compression is applied."""
    read_max_bytes: int
    """Maximum number of bytes that can be read in a single operation."""
    send_max_bytes: int
    """Maximum number of bytes that can be sent in a single operation."""
    require_connect_protocol_header: bool
    """Whether to require Connect protocol headers."""
    idempotency_level: IdempotencyLevel
    """The level of idempotency support for operations."""


class ProtocolClientParams(BaseModel):
    """Parameters for configuring a protocol client.

    This class defines the configuration parameters needed to create and operate
    a protocol client for network communication.

    Attributes:
        pool (AsyncConnectionPool): The connection pool for managing async connections.
        codec (Codec): The codec used for encoding/decoding messages.
        url (URL): The target URL for the protocol client.
        compression_name (str | None): The name of the compression algorithm to use.
            Defaults to None if no compression is specified.
        compressions (list[Compression]): List of available compression algorithms.
        compress_min_bytes (int): Minimum number of bytes required before applying compression.
        read_max_bytes (int): Maximum number of bytes that can be read in a single operation.
        send_max_bytes (int): Maximum number of bytes that can be sent in a single operation.
        enable_get (bool): Whether GET requests are enabled for this client.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    pool: AsyncConnectionPool
    """The connection pool for managing async connections."""
    codec: Codec
    """The codec used for encoding/decoding messages."""
    url: URL
    """The target URL for the protocol client."""
    compression_name: str | None = Field(default=None)
    """The name of the compression algorithm to use. Defaults to None if no compression is specified."""
    compressions: list[Compression]
    """List of available compression algorithms."""
    compress_min_bytes: int
    """Minimum number of bytes required before applying compression."""
    read_max_bytes: int
    """Maximum number of bytes that can be read in a single operation."""
    send_max_bytes: int
    """Maximum number of bytes that can be sent in a single operation."""
    enable_get: bool
    """Whether GET requests are enabled for this client."""


class ProtocolClient(abc.ABC):
    """Abstract base class for protocol clients that handle communication with remote services.

    This class defines the interface for protocol clients that manage connections,
    handle request headers, and provide peer information for different streaming protocols.

    The ProtocolClient serves as a foundation for implementing specific protocol
    handlers (such as HTTP/1.1, HTTP/2, or gRPC) while maintaining a consistent
    interface for client operations.
    """

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Get the peer information for this connection.

        Returns:
            Peer: The peer object containing information about the connected peer.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def write_request_headers(self, stream_type: StreamType, headers: Headers) -> None:
        """Write request headers to the stream.

        Args:
            stream_type (StreamType): The type of stream being used for the request.
            headers (Headers): The headers to be written to the request stream.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Establish a connection to a streaming service.

        Args:
            spec (Spec): The specification object containing connection details and configuration.
            headers (Headers): HTTP headers to be included with the connection request.

        Returns:
            StreamingClientConn: A streaming client connection object for communicating with the service.
        """
        raise NotImplementedError()


class ProtocolHandler(abc.ABC):
    """Abstract base class for defining protocol handlers.

    This class provides a standardized interface for different communication
    protocols, such as gRPC, Connect, and gRPC-Web. By subclassing
    `ProtocolHandler`, developers can integrate custom or standard protocols
    into the server framework.

    Subclasses must implement the abstract methods defined in this class to
    specify the supported HTTP methods, content types, and to provide the
    core logic for handling connections and processing requests.
    """

    @property
    @abc.abstractmethod
    def methods(self) -> list[HTTPMethod]:
        """Gets the HTTP methods that this protocol supports.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            A list of the supported HTTP methods.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def content_types(self) -> list[str]:
        """Gets the list of supported content types.

        This is an abstract method that must be implemented by subclasses. It should
        return a list of MIME types that the protocol implementation can handle.

        Returns:
            list[str]: A list of supported content type strings (e.g., "application/json").

        Raises:
            NotImplementedError: If the method is not overridden by a subclass.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Checks if the protocol can handle the request payload.

        This method determines whether the current protocol implementation is capable
        of processing the payload of a given request based on its content type.
        Subclasses must implement this method.

        Args:
            request (Request): The incoming request object.
            content_type (str): The content type of the request's payload.

        Returns:
            bool: True if the payload can be handled by this protocol, False otherwise.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def conn(
        self,
        request: Request,
        response_headers: Headers,
        response_trailers: Headers,
        writer: ServerResponseWriter,
    ) -> StreamingHandlerConn | None:
        """Initializes a streaming connection handler.

        This method is called by the server to begin handling a streaming RPC.
        It sets up the necessary context and returns a handler object that will
        process the incoming and outgoing messages for the stream.

        Args:
            request: The incoming request details.
            response_headers: A mutable headers object to which response headers should be written.
            response_trailers: A mutable headers object to which response trailers should be written.
            writer: The writer for sending response messages and trailers.

        Returns:
            An instance of a `StreamingHandlerConn` to handle the stream, or `None` to
            terminate the connection.
        """
        raise NotImplementedError()


class Protocol(abc.ABC):
    """Defines the abstract interface for a communication protocol.

    This abstract base class (ABC) establishes a contract for different
    communication protocols. It ensures that any protocol implementation
    provides a consistent way to create both a server-side handler and a
    client.

    Subclasses are required to implement the `handler` and `client` methods
    to provide the specific logic for their respective protocol.
    """

    @abc.abstractmethod
    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Gets the appropriate protocol handler for the given parameters.

        This method is intended to be overridden by subclasses to return a specific
        handler instance based on the provided parameters.

        Args:
            params: The parameters used to determine which handler to return.

        Returns:
            An instance of a class that implements the ProtocolHandler protocol.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Creates and returns a client instance for this protocol.

        This is an abstract method that must be implemented by subclasses. It should
        take the necessary parameters and return a fully configured client object
        capable of communicating using the defined protocol.

        Args:
            params: The parameters required to initialize the client.

        Returns:
            An instance of the protocol client.
        """
        raise NotImplementedError()


def mapped_method_handlers(handlers: list[ProtocolHandler]) -> dict[HTTPMethod, list[ProtocolHandler]]:
    """Groups protocol handlers by the HTTP methods they support.

    This function takes a flat list of protocol handlers and transforms it into a
    dictionary where keys are HTTP methods and values are lists of handlers
    that support that method.

    Args:
        handlers: A list of ProtocolHandler instances to be mapped.

    Returns:
        A dictionary mapping each HTTPMethod to a list of ProtocolHandlers
        that support it.
    """
    method_handlers: dict[HTTPMethod, list[ProtocolHandler]] = {}
    for handler in handlers:
        for method in handler.methods:
            method_handlers.setdefault(method, []).append(handler)

    return method_handlers


def negotiate_compression(
    available: list[Compression], sent: str | None, accept: str | None
) -> tuple[Compression | None, Compression | None, ConnectError | None]:
    """Negotiates the compression algorithms for the request and response.

    This function determines which compression algorithm to use for decompressing
    the request body and compressing the response body based on the client's
    headers and the server's available options.

    Args:
        available (list[Compression]): A list of compression algorithms supported by the server.
        sent (str | None): The value of the `connect-content-encoding` header from the request,
            indicating the compression used for the request body.
        accept (str | None): The value of the `connect-accept-encoding` header from the request,
            indicating the compression algorithms the client accepts for the response.

    Returns:
        tuple[Compression | None, Compression | None, ConnectError | None]: A tuple containing:
            - The compression algorithm for the request body (or None).
            - The compression algorithm for the response body (or None).
            - A ConnectError if the client sent an unsupported compression, otherwise None.
    """
    request = None
    response = None

    if sent is not None and sent != COMPRESSION_IDENTITY:
        found = next((c for c in available if c.name == sent), None)
        if found:
            request = found
        else:
            return (
                None,
                None,
                ConnectError(
                    f"unknown compression {sent}: supported encodings are {', '.join(c.name for c in available)}",
                    Code.UNIMPLEMENTED,
                ),
            )

    if accept is None or accept == "":
        response = request
    else:
        accept_names = [name.strip() for name in accept.split(",")]
        for name in accept_names:
            found = next((c for c in available if c.name == name), None)
            if found:
                response = found
                break

    return request, response, None


def sorted_allow_method_value(handlers: list[ProtocolHandler]) -> str:
    """Generates a sorted, comma-separated string of method values from handlers.

    This function aggregates all unique HTTP methods from a list of protocol
    handlers, sorts them alphabetically, and returns them as a single
    comma-separated string. This is typically used to generate the value for
    the `Allow` HTTP header.

    Args:
        handlers: A list of ProtocolHandler instances from which to extract
            HTTP methods.

    Returns:
        A string containing the sorted, unique HTTP method values,
        joined by ", ". For example: "GET, POST, PUT".
    """
    methods = {method for handler in handlers for method in handler.methods}
    return ", ".join(sorted(method.value for method in methods))


def sorted_accept_post_value(handlers: list[ProtocolHandler]) -> str:
    """Generates a sorted, comma-separated string of content types.

    This function takes a list of protocol handlers, collects all unique
    content types they support, sorts them alphabetically, and formats them
    into a single string suitable for an `Accept-Post` header.

    Args:
        handlers: A list of `ProtocolHandler` instances.

    Returns:
        A string containing the sorted, comma-separated list of
        supported content types.
    """
    content_types = {content_type for handler in handlers for content_type in handler.content_types()}
    return ", ".join(sorted(content_type for content_type in content_types))


def code_from_http_status(status: int) -> Code:
    """Converts an HTTP status code to a gRPC `Code`.

    This function implements the mapping from HTTP status codes to gRPC status codes
    as specified by the gRPC-HTTP2 mapping. It handles common error codes like 400,
    401, 403, 404, 429, and 5xx.

    Note that a 200 OK status is mapped to `Code.UNKNOWN` because a successful
    gRPC response over HTTP is expected to include a `grpc-status` header to
    indicate the true status.

    See: https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md.

    Args:
        status (int): The HTTP status code.

    Returns:
        Code: The corresponding gRPC status code.
    """
    match status:
        case 400:  # Bad Request
            return Code.INTERNAL
        case 401:  # Unauthorized
            return Code.UNAUTHENTICATED
        case 403:  # Forbidden
            return Code.PERMISSION_DENIED
        case 404:  # Not Found
            return Code.UNIMPLEMENTED
        case 429:  # Too Many Requests
            return Code.UNAVAILABLE
        case 502:  # Bad Gateway
            return Code.UNAVAILABLE
        case 503:  # Service Unavailable
            return Code.UNAVAILABLE
        case 504:  # Gateway Timeout
            return Code.UNAVAILABLE
        case _:  # 200 is UNKNOWN because there should be a grpc-status in case of truly OK response.
            return Code.UNKNOWN


def exclude_protocol_headers(headers: Headers) -> Headers:
    """Filters out protocol-specific headers from a Headers object.

    This function iterates through a given set of headers and creates a new
    Headers object that excludes common HTTP, Connect, and gRPC protocol
    headers. The resulting object contains only application-specific headers.

    Args:
        headers (Headers): The input headers to be filtered.

    Returns:
        Headers: A new Headers object containing only non-protocol headers.
    """
    non_protocol_headers = Headers(encoding=headers.encoding)
    for key, value in headers.items():
        if key.lower() not in [
            # HTTP headers.
            "content-type",
            "content-length",
            "content-encoding",
            "host",
            "user-agent",
            "trailer",
            "date",
            # Connect headers.
            "accept-encoding",
            "trailer-",
            "connect-content-encoding",
            "connect-accept-encoding",
            "connect-timeout-ms",
            "connect-protocol-version",
            # // gRPC headers.
            "Grpc-Status",
            "Grpc-Accept-Encoding",
            "Grpc-Timeout",
            "Grpc-Message",
            "Grpc-Status-Details-Bin",
        ]:
            non_protocol_headers[key] = value

    return non_protocol_headers
