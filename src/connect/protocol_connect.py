"""Provides classes and functions for handling protocol connections.

Handles data serialization/deserialization using the Connect protocol.
"""

from typing import Any

from starlette.datastructures import MutableHeaders

from connect.codec import Codec
from connect.compression import Compression
from connect.connect import Spec, StreamingHandlerConn, StreamType
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    HttpMethod,
    Protocol,
    ProtocolHandler,
    ProtocolHandlerParams,
    negotiate_compression,
)
from connect.request import Request
from connect.response import Response

CONNECT_UNARY_HEADER_COMPRESSION = "content-encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "accept-encoding"
CONNECT_HEADER_PROTOCOL_VERSION = "connect-protocol-version"
CONNECT_PROTOCOL_VERSION = "1"

CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"
CONNECT_UNARY_CONTENT_TYPE_JSON = "application/json"

CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"


def connect_codec_from_content_type(stream_type: StreamType, content_type: str) -> str:
    """Extract the codec from the content type based on the stream type.

    Args:
        stream_type (StreamType): The type of stream (Unary or Streaming).
        content_type (str): The content type string from which to extract the codec.

    Returns:
        str: The extracted codec from the content type.

    """
    if stream_type == StreamType.Unary:
        return content_type[len(CONNECT_UNARY_CONTENT_TYPE_PREFIX) :]

    return content_type[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]


class ConnectHandler(ProtocolHandler):
    """A handler for managing protocol connections.

    Attributes:
        params (ProtocolHandlerParams): Parameters for the protocol handler.
        __methods (list[HttpMethod]): List of HTTP methods supported by the handler.
        accept (list[str]): List of accepted content types.

    """

    params: ProtocolHandlerParams
    __methods: list[HttpMethod]
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HttpMethod], accept: list[str]) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            params (ProtocolHandlerParams): The parameters for the protocol handler.
            methods (list[HttpMethod]): A list of HTTP methods.
            accept (list[str]): A list of accepted content types.

        """
        self.params = params
        self.__methods = methods
        self.accept = accept

    def methods(self) -> list[HttpMethod]:
        """Return the list of HTTP methods.

        Returns:
            list[HttpMethod]: A list of HTTP methods.

        """
        return self.__methods

    def content_types(self) -> None:
        """Handle content types.

        This method currently does nothing and serves as a placeholder for future
        implementation related to content types.

        """
        pass

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Check if the handler can handle the payload."""
        if HttpMethod(request.method) == HttpMethod.GET:
            pass

        return content_type in self.accept

    async def conn(self, request: Request, response_headers: MutableHeaders) -> StreamingHandlerConn:
        """Handle the connection for the given request and response headers.

        Args:
            request (Request): The incoming request object.
            response_headers (MutableHeaders): The headers for the response.

        Returns:
            StreamingHandlerConn: The connection handler for the request.

        Raises:
            ValueError: If the request method is not supported or if the codec is not found.

        Note:
            - This method currently supports only Unary stream type.
            - Compression negotiation is performed based on request headers.
            - The actual request body is read for non-GET methods.
            - Streaming support is not yet implemented.

        """
        _query = request.url.query
        if self.params.spec.stream_type == StreamType.Unary:
            if HttpMethod(request.method) == HttpMethod.GET:
                # TODO(tsubakiky): Get the compression from the query parameter
                pass
            else:
                content_encoding = request.headers.get(CONNECT_UNARY_HEADER_COMPRESSION, None)

            accept_encoding = request.headers.get(CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION, None)

        # TODO(tsubakiky): Add validations

        request_compression, response_compression = negotiate_compression(
            self.params.compressions, content_encoding, accept_encoding
        )

        required = self.params.require_connect_protocol_header and self.params.spec.stream_type == StreamType.Unary
        connect_check_protocol_version(request, required)

        request_body: bytes
        if HttpMethod(request.method) == HttpMethod.GET:
            pass
        else:
            request_body = await request.body()
            content_type = request.headers.get(HEADER_CONTENT_TYPE, "")
            codec_name = connect_codec_from_content_type(self.params.spec.stream_type, content_type)

        codec = self.params.codecs.get(codec_name)
        if self.params.spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryHandlerConn(
                marshaler=ConnectMarshaler(
                    codec=codec,
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    compression=response_compression,
                    response_headers=response_headers,
                ),
                unmarshaler=ConnectUnmarshaler(
                    body=request_body,
                    codec=codec,
                    compression=request_compression,
                    read_max_bytes=self.params.read_max_bytes,
                ),
                response_headers=response_headers,
            )
        else:
            # TODO(tsubakiky): Add streaming support
            pass

        return conn


class ProtocolConnect(Protocol):
    """ProtocolConnect is a class that implements the Protocol interface for handling connection protocols."""

    def __init__(self) -> None:
        """Initialize the class instance."""
        pass

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        """Handle the creation of a ConnectHandler based on the provided ProtocolHandlerParams.

        Args:
            params (ProtocolHandlerParams): The parameters required to create the ConnectHandler.

        Returns:
            ConnectHandler: An instance of ConnectHandler configured with the appropriate methods and content types.

        """
        methods = [HttpMethod.POST]

        if params.spec.stream_type == StreamType.Unary:
            methods.append(HttpMethod.GET)

        content_types: list[str] = []
        for name in params.codecs.names():
            if params.spec.stream_type == StreamType.Unary:
                content_types.append(CONNECT_UNARY_CONTENT_TYPE_PREFIX + name)
                continue

            content_types.append(CONNECT_STREAMING_CONTENT_TYPE_PREFIX + name)

        return ConnectHandler(params, methods=methods, accept=content_types)

    def client(self) -> None:
        """Handle the client connection."""
        pass


class ConnectUnmarshaler:
    """A class to handle the unmarshaling of data using a specified codec.

    Attributes:
        codec (Codec): The codec used for unmarshaling the data.
        body (bytes): The raw data to be unmarshaled.
        read_max_bytes (int): The maximum number of bytes to read.
        compression (Compression | None): The compression method to use, if any.

    """

    codec: Codec
    body: bytes
    read_max_bytes: int
    compression: Compression | None

    def __init__(self, body: bytes, codec: Codec, read_max_bytes: int, compression: Compression | None) -> None:
        """Initialize the ProtocolConnect object.

        Args:
            body (bytes): The body of the message.
            codec (Codec): The codec used for encoding/decoding the message.
            read_max_bytes (int): The maximum number of bytes to read.
            compression (Compression | None): The compression method to use, if any.

        """
        self.body = body
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression

    def unmarshal(self, message: Any) -> Any:
        """Unmarshals the given message using the codec.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled object.

        """
        data = self.body
        if len(data) > 0 and self.compression:
            data = self.compression.decompress(data, self.read_max_bytes)

        obj = self.codec.unmarshal(data, message)
        return obj


class ConnectMarshaler:
    """ConnectMarshaler is responsible for serializing and optionally compressing messages.

    Attributes:
        codec (Codec): The codec used for serialization.
        compression (Compression | None): The compression method to use, if any.
        compress_min_bytes (int): The minimum number of bytes required to trigger compression.
        send_max_bytes (int): The maximum number of bytes that can be sent.
        response_headers (MutableHeaders): The headers to include in the response.

    """

    codec: Codec
    compression: Compression | None
    compress_min_bytes: int
    send_max_bytes: int
    response_headers: MutableHeaders

    def __init__(
        self,
        codec: Codec,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
        response_headers: MutableHeaders,
    ) -> None:
        """Initialize the ConnectMarshaler with the given parameters.

            compression (Compression | None): The compression method to use, or None if no compression is needed.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes that can be sent.
            response_headers (MutableHeaders): The headers to include in the response.

        Returns:
            None

        """
        self.codec = codec
        self.compression = compression
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.response_headers = response_headers

    def marshal(self, message: Any) -> bytes:
        """Serialize and optionally compresses a message.

        This method serializes the given message using the codec's marshal method.
        If the serialized data is smaller than the minimum compression size or if
        compression is not enabled, the data is returned as is. If the data size
        exceeds the maximum allowed size, a ValueError is raised.

        If compression is enabled and the serialized data is larger than the minimum
        compression size, the data is compressed. If the compressed data size exceeds
        the maximum allowed size, a ValueError is raised. The response headers are
        updated to include the compression type used.

        Args:
            message (Any): The message to be serialized and optionally compressed.

        Returns:
            bytes: The serialized (and possibly compressed) message.

        Raises:
            ValueError: If the serialized or compressed data exceeds the maximum allowed size.

        """
        data = self.codec.marshal(message)
        if len(data) < self.compress_min_bytes or self.compression is None:
            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                # TODO(tsubakiky): Add error handling
                raise ValueError("Data exceeds maximum size")

            return data

        data = self.compression.compress(data)
        if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
            # TODO(tsubakiky): Add error handling
            raise ValueError("Data exceeds maximum size")

        self.response_headers[CONNECT_UNARY_HEADER_COMPRESSION] = self.compression.name()

        return data


class ConnectUnaryHandlerConn(StreamingHandlerConn):
    """ConnectUnaryHandlerConn is a handler connection class for unary RPCs in the Connect protocol.

    Attributes:
        marshaler (ConnectMarshaler): An instance of ConnectMarshaler used to marshal messages.
        unmarshaler (ConnectUnmarshaler): An instance of ConnectUnmarshaler used to unmarshal messages.

    """

    marshaler: ConnectMarshaler
    unmarshaler: ConnectUnmarshaler
    response_headers: MutableHeaders

    def __init__(
        self, marshaler: ConnectMarshaler, unmarshaler: ConnectUnmarshaler, response_headers: MutableHeaders
    ) -> None:
        """Initialize the protocol connection.

        Args:
            marshaler (ConnectMarshaler): The marshaler to serialize data.
            unmarshaler (ConnectUnmarshaler): The unmarshaler to deserialize data.
            response_headers (MutableHeaders): The headers for the response.

        """
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_headers = response_headers

    def spec(self) -> Spec:
        """Retrieve the specification for the protocol.

        This method should be implemented by subclasses to provide the specific
        details of the protocol's specification.

        Returns:
            Spec: The specification of the protocol.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        """
        raise NotImplementedError

    def peer(self) -> Any:
        """Return the peer information.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            Any: The peer information.

        """
        raise NotImplementedError

    def receive(self, message: Any) -> Any:
        """Receives a message, unmarshals it, and returns the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled object.

        """
        obj = self.unmarshaler.unmarshal(message)
        return obj

    def request_header(self) -> Any:
        """Generate and return the request header.

        Returns:
            Any: The request header.

        """
        pass

    def send(self, message: Any) -> bytes:
        """Send a message by marshaling it into bytes.

        Args:
            message (Any): The message to be sent.

        Returns:
            bytes: The marshaled message in bytes.

        """
        data = self.marshaler.marshal(message)
        return data

    def response_header(self) -> Any:
        """Retrieve the response header.

        Returns:
            Any: The response header.

        """
        pass

    def response_trailer(self) -> Any:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The processed response trailer data.

        """
        pass

    def close(self, data: bytes) -> Response:
        """Close the connection and returns a response with the provided data.

        Args:
            data (bytes): The data to include in the response.

        Returns:
            Response: A response object containing the provided data, response headers, and a status code of 200.

        """
        response = Response(content=data, headers=self.response_headers, status_code=200)
        return response


def connect_check_protocol_version(request: Request, required: bool) -> None:
    """Check the protocol version in the request headers for POST requests.

    Args:
        request (Request): The incoming HTTP request.
        required (bool): Flag indicating whether the protocol version is required.

    Raises:
        ValueError: If the protocol version is required but not present in the headers.
        ValueError: If the protocol version is present but unsupported.
        ValueError: If the HTTP method is unsupported.

    """
    match HttpMethod(request.method):
        case HttpMethod.GET:
            pass
        case HttpMethod.POST:
            version = request.headers.get(CONNECT_HEADER_PROTOCOL_VERSION, None)
            if required and version is None:
                # TODO(tsubakiky): Add error handling
                raise ValueError("Protocol version is required")
            elif version is not None and version != CONNECT_PROTOCOL_VERSION:
                # TODO(tsubakiky): Add error handling
                raise ValueError("Unsupported protocol version")
        case _:
            # TODO(tsubakiky): Add error handling
            raise ValueError("Unsupported method")
