"""Module defining the protocol handling classes and functions."""

import abc
from http import HTTPMethod

from pydantic import BaseModel, ConfigDict, Field
from yarl import URL

from connect.code import Code
from connect.codec import Codec, ReadOnlyCodecs
from connect.compression import COMPRESSION_IDENTITY, Compression
from connect.connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamingHandlerConn,
    StreamType,
    UnaryClientConn,
)
from connect.error import ConnectError
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.request import Request
from connect.session import AsyncClientSession
from connect.writer import ServerResponseWriter

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
    """ProtocolHandlerParams is a data model that holds parameters for handling protocol operations.

    Attributes:
        spec (Spec): The specification details for the protocol.
        codecs (ReadOnlyCodecs): The codecs used for encoding and decoding data.
        compressions (list[Compression]): A list of compression methods to be used.
        compress_min_bytes (int): The minimum number of bytes required to trigger compression.
        read_max_bytes (int): The maximum number of bytes that can be read at once.
        send_max_bytes (int): The maximum number of bytes that can be sent at once.

    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    spec: Spec
    codecs: ReadOnlyCodecs
    compressions: list[Compression]
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    require_connect_protocol_header: bool
    idempotency_level: IdempotencyLevel


class ProtocolClientParams(BaseModel):
    """ProtocolClientParams is a data model for configuring protocol client parameters."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    session: AsyncClientSession
    codec: Codec
    url: URL
    compression_name: str | None = Field(default=None)
    compressions: list[Compression]
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    enable_get: bool


class ProtocolClient(abc.ABC):
    """Abstract base class for defining a protocol client."""

    @property
    @abc.abstractmethod
    def peer(self) -> Peer:
        """Retern the peer for the client."""
        raise NotImplementedError()

    @abc.abstractmethod
    def write_request_headers(self, stream_type: StreamType, headers: Headers) -> None:
        """Write the request headers."""
        raise NotImplementedError()

    @abc.abstractmethod
    def conn(self, spec: Spec, headers: Headers) -> UnaryClientConn:
        """Return the connection for the client."""
        raise NotImplementedError()

    @abc.abstractmethod
    def stream_conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Return the streaming connection for the client."""
        raise NotImplementedError()


HanderConn = StreamingHandlerConn


class ProtocolHandler(abc.ABC):
    """Abstract base class for handling different protocols."""

    @property
    @abc.abstractmethod
    def methods(self) -> list[HTTPMethod]:
        """Retrieve a list of HTTP methods.

        Returns:
            list[HTTPMethod]: A list of HTTP methods.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def content_types(self) -> list[str]:
        """Handle content types.

        This method currently does nothing and is intended to be implemented
        in the future to handle different content types as needed.

        Returns:
            None

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Determine if the payload of the given request can be handled based on the content type.

        Args:
            request (Request): The request object containing the payload.
            content_type (str): The content type of the payload.

        Returns:
            bool: True if the payload can be handled, False otherwise.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

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
        """Handle a connection request.

        Args:
            request (Request): The incoming request object.
            response_headers (Headers): The headers to be sent in the response.
            response_trailers (Headers): The trailers to be sent in the response.
            writer (ServerResponseWriter): The writer used to send the response.
            is_streaming (bool, optional): Whether this is a streaming connection. Defaults to False.

        Returns:
            StreamingHandlerConn | None: The connection handler or None if not implemented.

        Raises:
            NotImplementedError: If the method is not implemented.

        """
        raise NotImplementedError()


class Protocol(abc.ABC):
    """Abstract base class for defining a protocol.

    This class serves as a blueprint for creating protocol handlers and clients.
    Subclasses must implement the following abstract methods.

    """

    @abc.abstractmethod
    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Handle the protocol with the given parameters.

        Args:
            params (ProtocolHandlerParams): The parameters required to handle the protocol.

        Returns:
            ProtocolHandler: An instance of ProtocolHandler based on the provided parameters.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Implement client functionality.

        This method currently does nothing and is intended to be implemented
        in the future with the necessary client-side logic.
        """
        raise NotImplementedError()


def mapped_method_handlers(handlers: list[ProtocolHandler]) -> dict[HTTPMethod, list[ProtocolHandler]]:
    """Map protocol handlers to their respective HTTP methods.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        dict[HTTPMethod, list[ProtocolHandler]]: A dictionary where the keys are HTTP methods and the values are lists of protocol handlers that support those methods.

    """
    method_handlers: dict[HTTPMethod, list[ProtocolHandler]] = {}
    for handler in handlers:
        for method in handler.methods:
            method_handlers.setdefault(method, []).append(handler)

    return method_handlers


def negotiate_compression(
    available: list[Compression], sent: str | None, accept: str | None
) -> tuple[Compression | None, Compression | None, ConnectError | None]:
    """Negotiate the compression method to be used based on the available options.

    The compression method sent by the client, and the compression methods accepted
    by the server.

    Args:
        available (list[Compression]): A list of available compression methods.
        sent (str | None): The compression method sent by the client, or None if not specified.
        accept (str | None): A comma-separated string of compression methods accepted by the server, or None if not specified.
        header_name_accept_encoding (str): The name of the header used to specify the accepted compression methods.

    Returns:
        tuple[Compression | None, Compression | None]: A tuple containing the selected compression method for the request
        and the response. If no suitable compression method is found, None is returned for that position in the tuple.

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
    """Sort the allowed methods for a list of protocol handlers.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        str: A comma-separated string of the allowed methods.

    """
    methods = {method for handler in handlers for method in handler.methods}
    return ", ".join(sorted(method.value for method in methods))


def sorted_accept_post_value(handlers: list[ProtocolHandler]) -> str:
    """Sort the allowed methods for a list of protocol handlers.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        str: A comma-separated string of the allowed methods.

    """
    content_types = {content_type for handler in handlers for content_type in handler.content_types()}
    return ", ".join(sorted(content_type for content_type in content_types))


def code_from_http_status(status: int) -> Code:
    """Determine the gRPC-web error code for the given HTTP status code.

    See https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md.
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
    """Exclude protocol-specific headers from the given Headers object.

    This function filters out headers that are either standard HTTP headers
    or specific to the Connect protocol, and returns a new Headers object
    containing only the non-protocol headers.

    Args:
        headers (Headers): The original Headers object containing all headers.

    Returns:
        Headers: A new Headers object containing only the non-protocol headers.

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
            "Grpc-Status",
            "Grpc-Message",
            "Grpc-Status-Details-Bin",
        ]:
            non_protocol_headers[key] = value

    return non_protocol_headers
