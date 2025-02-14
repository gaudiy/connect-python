"""Provides classes and functions for handling protocol connections."""

import base64
import contextlib
import http
import json
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
)
from http import HTTPMethod, HTTPStatus
from sys import version
from typing import Any

import google.protobuf.any_pb2 as any_pb2
import httpcore
from google.protobuf import json_format
from yarl import URL

from connect.code import Code
from connect.codec import Codec, CodecNameType, StableCodec
from connect.compression import COMPRESSION_IDENTITY, Compression, get_compresion_from_name
from connect.connect import Address, Peer, Spec, StreamingClientConn, StreamingHandlerConn, StreamType, UnaryClientConn
from connect.envelope import Envelope, EnvelopeFlags
from connect.error import DEFAULT_ANY_RESOLVER_PREFIX, ConnectError, ErrorDetail
from connect.headers import Headers, include_request_headers
from connect.idempotency_level import IdempotencyLevel
from connect.protocol import (
    HEADER_CONTENT_ENCODING,
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    PROTOCOL_CONNECT,
    Protocol,
    ProtocolClient,
    ProtocolClientParams,
    ProtocolHandler,
    ProtocolHandlerParams,
    code_from_http_status,
    negotiate_compression,
)
from connect.request import Request
from connect.session import AsyncClientSession
from connect.utils import AsyncByteStream, map_httpcore_exceptions
from connect.version import __version__

CONNECT_UNARY_HEADER_COMPRESSION = "Content-Encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "Accept-Encoding"
CONNECT_UNARY_TRAILER_PREFIX = "Trailer-"
CONNECT_STREAMING_HEADER_COMPRESSION = "Connect-Content-Encoding"
CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION = "Connect-Accept-Encoding"
CONNECT_HEADER_TIMEOUT = "Connect-Timeout-Ms"
CONNECT_HEADER_PROTOCOL_VERSION = "Connect-Protocol-Version"
CONNECT_PROTOCOL_VERSION = "1"

CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_UNARY_CONTENT_TYPE_JSON = "application/json"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"

CONNECT_UNARY_ENCODING_QUERY_PARAMETER = "encoding"
CONNECT_UNARY_MESSAGE_QUERY_PARAMETER = "message"
CONNECT_UNARY_BASE64_QUERY_PARAMETER = "base64"
CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"
CONNECT_UNARY_CONNECT_QUERY_PARAMETER = "connect"
CONNECT_UNARY_CONNECT_QUERY_VALUE = "v" + CONNECT_PROTOCOL_VERSION

DEFAULT_CONNECT_USER_AGENT = f"connect-python/{__version__} (Python/{version})"


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


def connect_content_type_from_codec_name(stream_type: StreamType, codec_name: str) -> str:
    """Generate the content type string for a given stream type and codec name.

    Args:
        stream_type (StreamType): The type of the stream (e.g., Unary or Streaming).
        codec_name (str): The name of the codec.

    Returns:
        str: The content type string constructed from the stream type and codec name.

    """
    if stream_type == StreamType.Unary:
        return CONNECT_UNARY_CONTENT_TYPE_PREFIX + codec_name

    return CONNECT_STREAMING_CONTENT_TYPE_PREFIX + codec_name


class ConnectHandler(ProtocolHandler):
    """A handler for managing protocol connections.

    Attributes:
        params (ProtocolHandlerParams): Parameters for the protocol handler.
        __methods (list[HTTPMethod]): List of HTTP methods supported by the handler.
        accept (list[str]): List of accepted content types.

    """

    params: ProtocolHandlerParams
    _methods: list[HTTPMethod]
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HTTPMethod], accept: list[str]) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            params (ProtocolHandlerParams): The parameters for the protocol handler.
            methods (list[HTTPMethod]): A list of HTTP methods.
            accept (list[str]): A list of accepted content types.

        """
        self.params = params
        self._methods = methods
        self.accept = accept

    @property
    def methods(self) -> list[HTTPMethod]:
        """Return the list of HTTP methods.

        Returns:
            list[HTTPMethod]: A list of HTTP methods.

        """
        return self._methods

    def content_types(self) -> list[str]:
        """Handle content types.

        This method currently does nothing and serves as a placeholder for future
        implementation related to content types.

        """
        return self.accept

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Check if the handler can handle the payload."""
        if HTTPMethod(request.method) == HTTPMethod.GET:
            codec_name = request.query_params.get(CONNECT_UNARY_ENCODING_QUERY_PARAMETER, "")
            content_type = connect_content_type_from_codec_name(self.params.spec.stream_type, codec_name)

        return content_type in self.accept

    async def conn(
        self, request: Request, response_headers: Headers, response_trailers: Headers
    ) -> StreamingHandlerConn:
        """Handle the connection for the given request and response headers.

        Args:
            request (Request): The incoming request object.
            response_headers (Headers): The headers for the response.
            response_trailers (Headers): The headers for the response trailers.

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
        query_params = request.query_params

        if self.params.spec.stream_type == StreamType.Unary:
            if HTTPMethod(request.method) == HTTPMethod.GET:
                content_encoding = query_params.get(CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER, None)
            else:
                content_encoding = request.headers.get(CONNECT_UNARY_HEADER_COMPRESSION, None)

            accept_encoding = request.headers.get(CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION, None)
        else:
            # Streaming support is not yet implemented
            content_encoding = None
            accept_encoding = None

        request_compression, response_compression = negotiate_compression(
            self.params.compressions, content_encoding, accept_encoding
        )

        required = self.params.require_connect_protocol_header and self.params.spec.stream_type == StreamType.Unary
        connect_check_protocol_version(request, required)

        if HTTPMethod(request.method) == HTTPMethod.GET:
            encoding = query_params.get(CONNECT_UNARY_ENCODING_QUERY_PARAMETER)
            message = query_params.get(CONNECT_UNARY_MESSAGE_QUERY_PARAMETER)
            if encoding is None:
                raise ConnectError(
                    f"missing {CONNECT_UNARY_ENCODING_QUERY_PARAMETER} parameter",
                    Code.INVALID_ARGUMENT,
                )
            elif message is None:
                raise ConnectError(
                    f"missing {CONNECT_UNARY_MESSAGE_QUERY_PARAMETER} parameter",
                    Code.INVALID_ARGUMENT,
                )

            if query_params.get(CONNECT_UNARY_BASE64_QUERY_PARAMETER) == "1":
                decoded = base64.urlsafe_b64decode(message)
            else:
                decoded = message.encode("utf-8")

            async def stream() -> AsyncGenerator[bytes]:
                yield decoded

            request_stream = AsyncByteStream(aiterator=stream())
            codec_name = encoding
            content_type = connect_content_type_from_codec_name(self.params.spec.stream_type, codec_name)
        else:
            request_stream = AsyncByteStream(aiterator=request.stream())
            content_type = request.headers.get(HEADER_CONTENT_TYPE, "")
            codec_name = connect_codec_from_content_type(self.params.spec.stream_type, content_type)

        codec = self.params.codecs.get(codec_name)
        if codec is None:
            raise ConnectError(
                f"invalid message encoding: {codec_name}",
                Code.INVALID_ARGUMENT,
            )

        response_headers[HEADER_CONTENT_TYPE] = content_type
        accept_compression_header = CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION
        if self.params.spec.stream_type != StreamType.Unary:
            # TODO(tsubakiky): Add streaming support
            pass
        response_headers[accept_compression_header] = f"{', '.join(c.name for c in self.params.compressions)}"

        peer = Peer(
            address=Address(host=request.client.host, port=request.client.port) if request.client else request.client,
            protocol=PROTOCOL_CONNECT,
            query=request.query_params,
        )

        if self.params.spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryHandlerConn(
                request=request,
                peer=peer,
                spec=self.params.spec,
                marshaler=ConnectUnaryMarshaler(
                    codec=codec,
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    compression=response_compression,
                    headers=response_headers,
                ),
                unmarshaler=ConnectUnaryUnmarshaler(
                    stream=request_stream,
                    codec=codec,
                    compression=request_compression,
                    read_max_bytes=self.params.read_max_bytes,
                ),
                request_headers=Headers(request.headers, encoding="latin-1"),
                response_headers=response_headers,
                response_trailers=response_trailers,
            )
        else:
            # TODO(tsubakiky): Add streaming support
            pass

        return conn


class ProtocolConnect(Protocol):
    """ProtocolConnect is a class that implements the Protocol interface for handling connection protocols."""

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        """Handle the creation of a ConnectHandler based on the provided ProtocolHandlerParams.

        Args:
            params (ProtocolHandlerParams): The parameters required to create the ConnectHandler.

        Returns:
            ConnectHandler: An instance of ConnectHandler configured with the appropriate methods and content types.

        """
        methods = [HTTPMethod.POST]

        if params.spec.stream_type == StreamType.Unary and params.idempotency_level == IdempotencyLevel.NO_SIDE_EFFECTS:
            methods.append(HTTPMethod.GET)

        content_types: list[str] = []
        for name in params.codecs.names():
            if params.spec.stream_type == StreamType.Unary:
                content_types.append(CONNECT_UNARY_CONTENT_TYPE_PREFIX + name)
                continue

            content_types.append(CONNECT_STREAMING_CONTENT_TYPE_PREFIX + name)

        return ConnectHandler(params, methods=methods, accept=content_types)

    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Create and returns a ConnectClient instance.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the client.

        Returns:
            ProtocolClient: An instance of ConnectClient.

        """
        return ConnectClient(params)


class ConnectUnaryUnmarshaler:
    """A class to handle the unmarshaling of data using a specified codec.

    Attributes:
        codec (Codec): The codec used for unmarshaling the data.
        body (bytes): The raw data to be unmarshaled.
        read_max_bytes (int): The maximum number of bytes to read.
        compression (Compression | None): The compression method to use, if any.

    """

    codec: Codec
    read_max_bytes: int
    compression: Compression | None
    stream: AsyncByteStream | None

    def __init__(
        self,
        codec: Codec,
        read_max_bytes: int,
        compression: Compression | None = None,
        stream: AsyncByteStream | None = None,
    ) -> None:
        """Initialize the ProtocolConnect object.

        Args:
            stream (Callable[..., AsyncGenerator[bytes]]): The stream of data to be unmarshaled.
            codec (Codec): The codec used for encoding/decoding the message.
            read_max_bytes (int): The maximum number of bytes to read.
            compression (Compression | None): The compression method to use, if any.

        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream

    async def unmarshal(self, message: Any) -> Any:
        """Asynchronously unmarshals a given message using the provided unmarshal function and codec.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The result of the unmarshaling process.

        """
        return await self.unmarshal_func(message, self.codec.unmarshal)

    async def unmarshal_func(self, message: Any, func: Callable[[bytes, Any], Any]) -> Any:
        """Asynchronously unmarshals a message using the provided function.

        This function reads data from the stream in chunks, checks if the total
        bytes read exceed the maximum allowed bytes, and optionally decompresses
        the data. It then uses the provided function to unmarshal the data into
        the desired format.

        Args:
            message (Any): The message to be unmarshaled.
            func (Callable[[bytes, Any], Any]): A function that takes the raw bytes
                and the message, and returns the unmarshaled object.

        Returns:
            Any: The unmarshaled object.

        Raises:
            ConnectError: If the stream is not set, if the message size exceeds the
                maximum allowed bytes, or if there is an error during unmarshaling.

        """
        if self.stream is None:
            raise ConnectError("stream is not set", Code.INTERNAL)

        chunks: list[bytes] = []
        bytes_read = 0
        # TODO(tsubakiky): close the stream
        async for chunk in self.stream:
            chunk_size = len(chunk)
            bytes_read += chunk_size
            if self.read_max_bytes > 0 and bytes_read > self.read_max_bytes:
                raise ConnectError(
                    f"message size {bytes_read} is larger than configured max {self.read_max_bytes}",
                    Code.RESOURCE_EXHAUSTED,
                )

            chunks.append(chunk)

        data = b"".join(chunks)

        if len(data) > 0 and self.compression:
            data = self.compression.decompress(data, self.read_max_bytes)

        try:
            obj = func(data, message)
        except Exception as e:
            raise ConnectError(
                f"unmarshal message: {str(e)}",
                Code.INVALID_ARGUMENT,
            ) from e

        return obj


class ConnectUnaryMarshaler:
    """ConnectUnaryMarshaler is responsible for serializing and optionally compressing messages.

    Attributes:
        codec (Codec): The codec used for serializing messages.
        compression (Compression | None): The compression method used for compressing messages, if any.
        compress_min_bytes (int): The minimum size in bytes for a message to be compressed.
        send_max_bytes (int): The maximum allowed size in bytes for a message to be sent.
        headers (Headers | Headers): The headers to be included in the message.

    """

    codec: Codec
    compression: Compression | None
    compress_min_bytes: int
    send_max_bytes: int
    headers: Headers

    def __init__(
        self,
        codec: Codec,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
        headers: Headers,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            codec (Codec): The codec to be used for encoding/decoding.
            compression (Compression | None): The compression method to be used, or None if no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes to send in a single message.
            headers (Headers): The headers to be included in the connection.

        Returns:
            None

        """
        self.codec = codec
        self.compression = compression
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.headers = headers

    def marshal(self, message: Any) -> bytes:
        """Marshals a message into bytes, optionally compressing it if it exceeds a certain size.

        Args:
            message (Any): The message to be marshaled.

        Returns:
            bytes: The marshaled (and possibly compressed) message.

        Raises:
            ConnectError: If there is an error during marshaling or if the message size exceeds the allowed limit.

        """
        try:
            data = self.codec.marshal(message)
        except Exception as e:
            raise ConnectError(f"marshal message: {str(e)}", Code.INTERNAL) from e

        if len(data) < self.compress_min_bytes or self.compression is None:
            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                raise ConnectError(
                    f"message size {len(data)} exceeds send_mas_bytes {self.send_max_bytes}", Code.RESOURCE_EXHAUSTED
                )

            return data

        data = self.compression.compress(data)

        if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
            raise ConnectError(
                f"compressed message size {len(data)} exceeds send_mas_bytes {self.send_max_bytes}",
                Code.RESOURCE_EXHAUSTED,
            )

        self.headers[CONNECT_UNARY_HEADER_COMPRESSION] = self.compression.name

        return data


class ConnectUnaryHandlerConn(StreamingHandlerConn):
    """ConnectUnaryHandlerConn is a handler connection class for unary RPCs in the Connect protocol.

    Attributes:
        request (Request): The incoming request object.
        marshaler (ConnectUnaryMarshaler): An instance of ConnectUnaryMarshaler used to marshal messages.
        unmarshaler (ConnectUnaryUnmarshaler): An instance of ConnectUnaryUnmarshaler used to unmarshal messages.
        headers (Headers): The headers for the response.

    """

    request: Request
    _peer: Peer
    _spec: Spec
    marshaler: ConnectUnaryMarshaler
    unmarshaler: ConnectUnaryUnmarshaler
    _request_headers: Headers
    _response_headers: Headers
    _response_trailers: Headers

    def __init__(
        self,
        request: Request,
        peer: Peer,
        spec: Spec,
        marshaler: ConnectUnaryMarshaler,
        unmarshaler: ConnectUnaryUnmarshaler,
        request_headers: Headers,
        response_headers: Headers,
        response_trailers: Headers | None = None,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            request (Request): The incoming request object.
            peer (Peer): The peer information.
            spec (Spec): The specification object.
            marshaler (ConnectUnaryMarshaler): The marshaler to serialize data.
            unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler to deserialize data.
            request_headers (Headers): The headers for the request.
            response_headers (Headers): The headers for the response.
            response_trailers (Headers, optional): The trailers for the response.

        """
        self.request = request
        self._peer = peer
        self._spec = spec
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers or Headers()

    @property
    def spec(self) -> Spec:
        """Return the specification object.

        Returns:
            Spec: The specification object.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer associated with this instance.

        :return: The peer associated with this instance.
        :rtype: Peer
        """
        return self._peer

    async def receive(self, message: Any) -> Any:
        """Receives a message, unmarshals it, and returns the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled object.

        """
        obj = await self.unmarshaler.unmarshal(message)
        return obj

    @property
    def request_headers(self) -> Headers:
        """Retrieve the headers from the request.

        Returns:
            Mapping[str, str]: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    def send(self, message: Any) -> bytes:
        """Send a message by marshaling it into bytes.

        Args:
            message (Any): The message to be sent.

        Returns:
            bytes: The marshaled message in bytes.

        """
        data = self.marshaler.marshal(message)
        return data

    @property
    def response_headers(self) -> Headers:
        """Retrieve the response headers.

        Returns:
            Any: The response headers.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The processed response trailer data.

        """
        return self._response_trailers

    def get_http_method(self) -> HTTPMethod:
        """Retrieve the HTTP method from the request.

        Returns:
            HTTPMethod: The HTTP method from the request.

        """
        return HTTPMethod(self.request.method)


class ConnectClient(ProtocolClient):
    """ConnectClient is a client for handling connections using the Connect protocol.

    Attributes:
        params (ProtocolClientParams): Parameters for the protocol client.
        _peer (Peer): The peer object representing the connection endpoint.

    """

    params: ProtocolClientParams
    _peer: Peer

    def __init__(self, params: ProtocolClientParams) -> None:
        """Initialize the ProtocolConnect instance with the given parameters.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the ProtocolConnect instance.

        """
        self.params = params
        self._peer = Peer(
            address=Address(host=params.url.host or "", port=params.url.port or 80),
            protocol=PROTOCOL_CONNECT,
            query={},
        )

    @property
    def peer(self) -> Peer:
        """Return the peer associated with this instance.

        :return: The peer associated with this instance.
        :rtype: Peer
        """
        return self._peer

    def write_request_headers(self, stream_type: StreamType, headers: Headers) -> None:
        """Write the necessary request headers to the provided headers dictionary.

        This method ensures that the headers dictionary contains the required headers
        for a request, including user agent, protocol version, content type, and
        optionally, compression settings.

        Args:
            stream_type (StreamType): The type of stream for the request.
            headers (Headers): The dictionary of headers to be updated.

        Returns:
            None

        """
        if headers.get(HEADER_USER_AGENT, None) is None:
            headers[HEADER_USER_AGENT] = DEFAULT_CONNECT_USER_AGENT

        headers[CONNECT_HEADER_PROTOCOL_VERSION] = CONNECT_PROTOCOL_VERSION
        headers[HEADER_CONTENT_TYPE] = connect_content_type_from_codec_name(stream_type, self.params.codec.name)

        accept_compression_header = CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION
        if stream_type != StreamType.Unary:
            headers[CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION] = COMPRESSION_IDENTITY
            accept_compression_header = CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION
            if self.params.compression_name and self.params.compression_name != COMPRESSION_IDENTITY:
                headers[CONNECT_STREAMING_HEADER_COMPRESSION] = self.params.compression_name

        if self.params.compressions:
            headers[accept_compression_header] = ", ".join(c.name for c in self.params.compressions)

    def conn(self, spec: Spec, headers: Headers) -> UnaryClientConn:
        """Establish a unary client connection with the given specifications and headers.

        Args:
            spec (Spec): The specification for the connection.
            headers (Headers): The headers to be included in the request.

        Returns:
            UnaryClientConn: The established unary client connection.

        """
        conn = ConnectUnaryClientConn(
            session=self.params.session,
            spec=spec,
            peer=self.peer,
            url=self.params.url,
            compressions=self.params.compressions,
            request_headers=headers,
            marshaler=ConnectUnaryRequestMarshaler(
                connect_marshaler=ConnectUnaryMarshaler(
                    codec=self.params.codec,
                    compression=get_compresion_from_name(self.params.compression_name, self.params.compressions),
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    headers=headers,
                )
            ),
            unmarshaler=ConnectUnaryUnmarshaler(
                codec=self.params.codec,
                read_max_bytes=self.params.read_max_bytes,
            ),
        )
        if spec.idempotency_level == IdempotencyLevel.NO_SIDE_EFFECTS:
            conn.marshaler.enable_get = self.params.enable_get
            conn.marshaler.url = self.params.url
            if isinstance(self.params.codec, StableCodec):
                conn.marshaler.stable_codec = self.params.codec

        return conn

    def stream_conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Establish a streaming connection using the provided specification and headers.

        Args:
            spec (Spec): The specification for the streaming connection.
            headers (Headers): The headers to be included in the connection request.

        Returns:
            StreamingClientConn: An instance of the streaming client connection.

        """
        conn = ConnectStreamingClientConn(
            session=self.params.session,
            spec=spec,
            peer=self.peer,
            url=self.params.url,
            compressions=self.params.compressions,
            request_headers=headers,
            marshaler=ConnectStreamingMarshaler(
                codec=self.params.codec,
                compress_min_bytes=self.params.compress_min_bytes,
                send_max_bytes=self.params.send_max_bytes,
                compression=get_compresion_from_name(self.params.compression_name, self.params.compressions),
            ),
            unmarshaler=ConnectStreamingUnmarshaler(
                codec=self.params.codec,
                read_max_bytes=self.params.read_max_bytes,
            ),
        )

        return conn


class ConnectUnaryRequestMarshaler:
    """A class responsible for marshaling unary requests using a provided ConnectUnaryMarshaler.

    Attributes:
        connect_marshaler (ConnectUnaryMarshaler): An instance of ConnectUnaryMarshaler used to marshal messages.

    """

    connect_marshaler: ConnectUnaryMarshaler
    enable_get: bool
    stable_codec: StableCodec | None
    url: URL | None

    def __init__(
        self,
        connect_marshaler: ConnectUnaryMarshaler,
        enable_get: bool = False,
        stable_codec: StableCodec | None = None,
        url: URL | None = None,
    ) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            connect_marshaler (ConnectUnaryMarshaler): The marshaler used for connecting.
            enable_get (bool, optional): Flag to enable GET requests. Defaults to False.
            stable_codec (StableCodec | None, optional): The codec to use for stable connections. Defaults to None.
            url (URL | None, optional): The URL for the connection. Defaults to None.

        """
        self.connect_marshaler = connect_marshaler
        self.enable_get = enable_get
        self.stable_codec = stable_codec
        self.url = url

    def marshal(self, message: Any) -> bytes:
        """Marshal a message into bytes.

        If `enable_get` is True and `stable_codec` is None, raises a `ConnectError`
        indicating that the codec does not support stable marshal and cannot use get.
        Otherwise, if `enable_get` is True and `stable_codec` is not None, marshals
        the message using the `marshal_with_get` method.

        If `enable_get` is False, marshals the message using the `connect_marshaler`.

        Args:
            message (Any): The message to be marshaled.

        Returns:
            bytes: The marshaled message in bytes.

        Raises:
            ConnectError: If `enable_get` is True and `stable_codec` is None.

        """
        if self.enable_get:
            if self.stable_codec is None:
                raise ConnectError(
                    f"codec {self.connect_marshaler.codec.name} doesn't support stable marshal; can't use get",
                    Code.INTERNAL,
                )
            else:
                return self.marshal_with_get(message)

        return self.connect_marshaler.marshal(message)

    def marshal_with_get(self, message: Any) -> bytes:
        """Marshals the given message and sends it using a GET request.

        This method first marshals the message using the stable codec. If the marshaled
        data exceeds the maximum allowed size (`send_max_bytes`) and compression is not
        enabled, it raises a `ConnectError`. If the data size is within the limit, it
        builds the GET URL and sends the data.

        If the data size exceeds the limit and compression is enabled, it compresses
        the data and checks the size again. If the compressed data still exceeds the
        limit, it raises a `ConnectError`. Otherwise, it builds the GET URL with the
        compressed data and sends it.

        Args:
            message (Any): The message to be marshaled and sent.

        Returns:
            bytes: The marshaled (and possibly compressed) data.

        Raises:
            ConnectError: If the data size exceeds the maximum allowed size and compression
                          is not enabled, or if the compressed data size still exceeds the
                          limit.

        """
        assert self.stable_codec is not None

        data = self.stable_codec.marshal_stable(message)

        is_too_big = self.connect_marshaler.send_max_bytes > 0 and len(data) > self.connect_marshaler.send_max_bytes
        if is_too_big and not self.connect_marshaler.compression:
            raise ConnectError(
                f"message size {len(data)} exceeds sendMaxBytes {self.connect_marshaler.send_max_bytes}: enabling request compression may help",
                Code.RESOURCE_EXHAUSTED,
            )

        if not is_too_big:
            url = self._build_get_url(data, False)

            self._write_with_get(url)
            return data

        assert self.connect_marshaler.compression
        data = self.connect_marshaler.compression.compress(data)

        if self.connect_marshaler.send_max_bytes > 0 and len(data) > self.connect_marshaler.send_max_bytes:
            raise ConnectError(
                f"compressed message size {len(data)} exceeds send_mas_bytes {self.connect_marshaler.send_max_bytes}",
                Code.RESOURCE_EXHAUSTED,
            )

        url = self._build_get_url(data, True)
        self._write_with_get(url)

        return data

    def _build_get_url(self, data: bytes, compressed: bool) -> URL:
        assert self.url is not None
        assert self.stable_codec is not None

        url = self.url
        url = url.update_query({
            CONNECT_UNARY_CONNECT_QUERY_PARAMETER: CONNECT_UNARY_CONNECT_QUERY_VALUE,
            CONNECT_UNARY_ENCODING_QUERY_PARAMETER: self.connect_marshaler.codec.name,
        })
        if self.stable_codec.is_binary() or compressed:
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8"),
                CONNECT_UNARY_BASE64_QUERY_PARAMETER: "1",
            })
        else:
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: data.decode("utf-8"),
            })

        if compressed:
            if not self.connect_marshaler.compression:
                raise ConnectError(
                    "compression must be set for compressed message",
                    Code.INTERNAL,
                )

            url = url.update_query({CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER: self.connect_marshaler.compression.name})

        return url

    def _write_with_get(self, url: URL) -> None:
        with contextlib.suppress(Exception):
            del self.connect_marshaler.headers[CONNECT_HEADER_PROTOCOL_VERSION]
            del self.connect_marshaler.headers[HEADER_CONTENT_TYPE]
            del self.connect_marshaler.headers[HEADER_CONTENT_ENCODING]
            del self.connect_marshaler.headers[HEADER_CONTENT_LENGTH]

        self.url = url


class ResponseAsyncByteStream(AsyncByteStream):
    """An asynchronous byte stream for reading and writing byte chunks."""

    aiterator: AsyncIterable[bytes] | None
    aclose_func: Callable[..., Awaitable[None]] | None

    def __init__(
        self,
        aiterator: AsyncIterable[bytes] | None = None,
        aclose_func: Callable[..., Awaitable[None]] | None = None,
    ) -> None:
        """Initialize the protocol connect instance.

        Args:
            aiterator (AsyncIterable[bytes] | None): An optional asynchronous iterable of bytes.
            aclose_func (Callable[..., Awaitable[None]] | None): An optional asynchronous close function.

        Returns:
            None

        """
        super().__init__(aiterator=aiterator, aclose_func=aclose_func)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronous iterator method to read byte chunks from the stream."""
        if self.aiterator is not None:
            with map_httpcore_exceptions():
                async for chunk in self.aiterator:
                    yield chunk

    async def aclose(self) -> None:
        """Asynchronously close the stream."""
        if self.aclose_func is not None:
            with map_httpcore_exceptions():
                await self.aclose_func()


class ConnectStreamingMarshaler:
    """A class responsible for marshaling messages with optional compression.

    Attributes:
        codec (Codec): The codec used for marshaling messages.
        compression (Compression | None): The compression method used for compressing messages, if any.

    """

    codec: Codec
    compress_min_bytes: int
    send_max_bytes: int
    compression: Compression | None

    def __init__(
        self, codec: Codec, compression: Compression | None, compress_min_bytes: int, send_max_bytes: int
    ) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            codec (Codec): The codec to be used for encoding and decoding.
            compression (Compression | None): The compression method to be used, or None if no compression is to be applied.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes that can be sent in a single message.

        """
        self.codec = codec
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.compression = compression

    async def marshal(self, messages: AsyncIterator[Any]) -> AsyncIterator[bytes]:
        """Asynchronously marshals and compresses messages from an asynchronous iterator.

        Args:
            messages (AsyncIterator[Any]): An asynchronous iterator of messages to be marshaled.

        Yields:
            AsyncIterator[bytes]: An asynchronous iterator of marshaled and optionally compressed messages in bytes.

        Raises:
            ConnectError: If there is an error during marshaling or if the message size exceeds the allowed limit.

        """
        async for message in messages:
            try:
                data = self.codec.marshal(message)
            except Exception as e:
                raise ConnectError(f"marshal message: {str(e)}", Code.INTERNAL) from e

            env = Envelope(data, EnvelopeFlags(0))

            if env.is_set(EnvelopeFlags.compressed) or self.compression is None or len(data) < self.compress_min_bytes:
                if self.send_max_bytes > 0 and len(env.data) > self.send_max_bytes:
                    raise ConnectError(
                        f"message size {len(data)} exceeds sendMaxBytes {self.send_max_bytes}", Code.RESOURCE_EXHAUSTED
                    )
                compressed_data = env.data
            else:
                compressed_data = self.compression.compress(data)
                env.flags |= EnvelopeFlags.compressed

                if self.send_max_bytes > 0 and len(env.data) > self.send_max_bytes:
                    raise ConnectError(
                        f"compressed message size {len(data)} exceeds send_mas_bytes {self.send_max_bytes}",
                        Code.RESOURCE_EXHAUSTED,
                    )

            env.data = compressed_data
            yield env.encode()


class ConnectStreamingUnmarshaler:
    """A class to handle the unmarshaling of streaming data.

    Attributes:
        codec (Codec): The codec used for unmarshaling data.
        compression (Compression | None): The compression method used, if any.
        stream (AsyncByteStream | None): The asynchronous byte stream to read data from.
        buffer (bytes): The buffer to store incoming data chunks.

    """

    codec: Codec
    read_max_bytes: int
    compression: Compression | None
    stream: AsyncByteStream | None
    buffer: bytes
    _end_stream_error: ConnectError | None
    _trailers: Headers

    def __init__(
        self,
        codec: Codec,
        read_max_bytes: int,
        stream: AsyncByteStream | None = None,
        compression: Compression | None = None,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            codec (Codec): The codec to use for encoding and decoding data.
            read_max_bytes (int): The maximum number of bytes to read from the stream.
            stream (AsyncByteStream | None, optional): The asynchronous byte stream to read from. Defaults to None.
            compression (Compression | None, optional): The compression method to use. Defaults to None.

        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream
        self.buffer = b""
        self._end_stream_error = None
        self._trailers = Headers()

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Asynchronously unmarshals messages from the stream.

        Args:
            message (Any): The message type to unmarshal.

        Yields:
            Any: The unmarshaled message object.

        Raises:
            ConnectError: If the stream is not set, if there is an error in the
                          unmarshaling process, or if there is a protocol error.

        """
        if self.stream is None:
            raise ConnectError("stream is not set", Code.INTERNAL)

        try:
            async for chunk in self.stream:
                end_stream_received = False
                self.buffer += chunk

                while True:
                    env, data_len = Envelope.decode(self.buffer)
                    if env is None:
                        break

                    if self.read_max_bytes > 0 and data_len > self.read_max_bytes:
                        raise ConnectError(
                            f"message size {data_len} is larger than configured readMaxBytes {self.read_max_bytes}",
                            Code.RESOURCE_EXHAUSTED,
                        )

                    self.buffer = self.buffer[5 + data_len :]

                    if env.is_set(EnvelopeFlags.end_stream):
                        if end_stream_received:
                            raise ConnectError("protocol error: multiple end stream flags", Code.INTERNAL)

                        error, trailers = end_stream_from_bytes(env.data)
                        self._end_stream_error = error
                        self._trailers = trailers
                        end_stream_received = True
                        obj = None
                    else:
                        if env.is_set(EnvelopeFlags.compressed):
                            if not self.compression:
                                raise ConnectError(
                                    "protocol error: sent compressed message without compression support", Code.INTERNAL
                                )

                            env.data = self.compression.decompress(env.data, self.read_max_bytes)

                        try:
                            obj = self.codec.unmarshal(env.data, message)
                        except Exception as e:
                            raise ConnectError(
                                f"unmarshal message: {str(e)}",
                                Code.INVALID_ARGUMENT,
                            ) from e

                    yield obj, end_stream_received
        finally:
            await self.stream.aclose()

            if len(self.buffer) > 0:
                header = Envelope.decode_header(self.buffer)
                if header:
                    message = f"protocol error: promised {header[1]} bytes in enveloped message, got {len(self.buffer) - 5} bytes"
                    raise ConnectError(message, Code.INVALID_ARGUMENT)

    @property
    def trailers(self) -> Headers:
        """Return the trailers headers.

        Trailers are additional headers sent after the body of the message.

        Returns:
            Headers: The trailers headers.

        """
        return self._trailers

    @property
    def end_stream_error(self) -> ConnectError | None:
        """Return the error that occurred at the end of the stream, if any.

        Returns:
            ConnectError | None: The error that occurred at the end of the stream,
            or None if no error occurred.

        """
        return self._end_stream_error


EventHook = Callable[..., Any]


class ConnectStreamingClientConn(StreamingClientConn):
    """ConnectStreamingClientConn is a class that manages a streaming client connection using the Connect protocol."""

    _spec: Spec
    _peer: Peer
    url: URL
    compressions: list[Compression]
    marshaler: ConnectStreamingMarshaler
    unmarshaler: ConnectStreamingUnmarshaler
    response_content: bytes | None
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers

    def __init__(
        self,
        session: AsyncClientSession,
        spec: Spec,
        peer: Peer,
        url: URL,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: ConnectStreamingMarshaler,
        unmarshaler: ConnectStreamingUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initialize a new instance of the class.

        Args:
            spec (Spec): The specification object.
            peer (Peer): The peer object.
            url (URL): The URL for the connection.
            compressions (list[Compression]): List of compression methods.
            request_headers (Headers): The headers for the request.
            marshaler (ConnectStreamingMarshaler): The marshaler for streaming.
            unmarshaler (ConnectStreamingUnmarshaler): The unmarshaler for streaming.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Event hooks for request and response. Defaults to None.

        Returns:
            None

        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.session = session
        self._spec = spec
        self._peer = peer
        self.url = url
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_content = None
        self._response_headers = Headers()
        self._response_trailers = Headers()
        self._request_headers = request_headers
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    @property
    def spec(self) -> Spec:
        """Return the specification of the protocol.

        Returns:
            Spec: The specification object of the protocol.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer object associated with this instance.

        :return: The peer object.
        :rtype: Peer
        """
        return self._peer

    @property
    def request_headers(self) -> Headers:
        """Retrieve the request headers.

        Returns:
            Headers: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    @property
    def response_headers(self) -> Headers:
        """Return the response headers.

        Returns:
            Headers: A dictionary-like object containing the response headers.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Return the response trailers.

        Response trailers are additional headers sent after the response body.

        Returns:
            Headers: A dictionary containing the response trailers.

        """
        return self._response_trailers

    def on_request_send(self, fn: EventHook) -> None:
        """Register a callback function to be called when a request is sent.

        Args:
            fn (EventHook): The callback function to be registered. This function
                            will be called with the request details when a request
                            is sent.

        """
        self._event_hooks["request"].append(fn)

    async def receive(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives and processes a message.

        Args:
            message (Any): The message to be processed.

        Yields:
            Any: Objects obtained from unmarshaling the message.

        """
        end_stream_received = False
        async for obj, end in self.unmarshaler.unmarshal(message):
            if end:
                if end_stream_received:
                    raise ConnectError("received extra end stream message", Code.INVALID_ARGUMENT)

                end_stream_received = True
                error = self.unmarshaler.end_stream_error
                if error:
                    for key, value in self.response_headers.items():
                        error.metadata[key] = value
                        error.metadata.update(self.unmarshaler.trailers.copy())
                    raise error

                for key, value in self.unmarshaler.trailers.items():
                    self.response_trailers[key] = value

                continue

            if end_stream_received:
                raise ConnectError("received message after end stream", Code.INVALID_ARGUMENT)

            yield obj

        if not end_stream_received:
            raise ConnectError("missing end stream message", Code.INVALID_ARGUMENT)

    async def send(self, messages: AsyncIterator[Any]) -> None:
        """Send a series of messages asynchronously.

        This method marshals the provided messages, constructs an HTTP POST request,
        and sends it using the httpcore library. It also triggers any registered
        request and response hooks, and validates the response.

        Args:
            messages (AsyncIterator[Any]): An asynchronous iterator of messages to be sent.

        Returns:
            None

        Raises:
            Exception: If there is an error during the request or response handling.

        """
        content_iterator = self.marshaler.marshal(messages)

        request = httpcore.Request(
            method=HTTPMethod.POST,
            url=httpcore.URL(
                scheme=self.url.scheme,
                host=self.url.host or "",
                port=self.url.port,
                target=self.url.raw_path,
            ),
            headers=list(
                # TODO(tsubakiky): update _request_headers
                include_request_headers(
                    headers=self._request_headers, url=self.url, content=content_iterator, method=HTTPMethod.POST
                ).items()
            ),
            content=content_iterator,
        )

        for hook in self._event_hooks["request"]:
            hook(request)

        with map_httpcore_exceptions():
            response = await self.session.pool.handle_async_request(request)

        for hook in self._event_hooks["response"]:
            hook(response)

        if response.status != http.HTTPStatus.OK:
            # TODO(tsubakiky): add error handling
            await response.aread()

        self.unmarshaler.stream = ResponseAsyncByteStream(
            aiterator=response.aiter_stream(), aclose_func=response.aclose
        )

        self._validate_response(response)

        return

    def _validate_response(self, response: httpcore.Response) -> None:
        response_headers = Headers(response.headers)

        compression = response_headers.get(CONNECT_STREAMING_HEADER_COMPRESSION, None)
        if (
            compression
            and compression != COMPRESSION_IDENTITY
            and not any(c.name == compression for c in self.compressions)
        ):
            raise ConnectError(
                f"unknown encoding {compression}: accepted encodings are {', '.join(c.name for c in self.compressions)}",
                Code.INTERNAL,
            )

        self.unmarshaler.compression = get_compresion_from_name(compression, self.compressions)
        self._response_headers.update(response_headers)


class ConnectUnaryClientConn(UnaryClientConn):
    """A client connection for unary RPCs using the Connect protocol.

    Attributes:
        _spec (Spec): The specification for the connection.
        _peer (Peer): The peer information.
        url (URL): The URL for the connection.
        compressions (list[Compression]): List of supported compressions.
        marshaler (ConnectUnaryRequestMarshaler): The marshaler for requests.
        unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler for responses.
        response_content (bytes | None): The content of the response.
        _response_headers (Headers): The headers of the response.
        _response_trailers (Headers): The trailers of the response.
        _request_headers (Headers): The headers of the request.
        _event_hooks (dict[str, list[EventHook]]): Event hooks for request and response.

    """

    session: AsyncClientSession
    _spec: Spec
    _peer: Peer
    url: URL
    compressions: list[Compression]
    marshaler: ConnectUnaryRequestMarshaler
    unmarshaler: ConnectUnaryUnmarshaler
    response_content: bytes | None
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers

    def __init__(
        self,
        session: AsyncClientSession,
        spec: Spec,
        peer: Peer,
        url: URL,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: ConnectUnaryRequestMarshaler,
        unmarshaler: ConnectUnaryUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initialize the ConnectProtocol instance.

        Args:
            spec (Spec): The specification for the connection.
            peer (Peer): The peer information.
            url (URL): The URL for the connection.
            compressions (list[Compression]): List of compression methods.
            request_headers (Headers): The headers for the request.
            marshaler (ConnectUnaryRequestMarshaler): The marshaler for the request.
            unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler for the response.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Event hooks for request and response. Defaults to None.

        Returns:
            None

        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.session = session
        self._spec = spec
        self._peer = peer
        self.url = url
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_content = None
        self._response_headers = Headers()
        self._response_trailers = Headers()
        self._request_headers = request_headers
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    @property
    def spec(self) -> Spec:
        """Return the specification of the protocol.

        Returns:
            Spec: The specification object of the protocol.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer object associated with this instance.

        :return: The peer object.
        :rtype: Peer
        """
        return self._peer

    async def receive(self, message: Any) -> Any:
        """Asynchronously receives a message, unmarshals it, and returns the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            None: This method does not return a value. The unmarshaled object is returned implicitly.

        """
        obj = await self.unmarshaler.unmarshal(message)
        return obj

    @property
    def request_headers(self) -> Headers:
        """Retrieve the request headers.

        Returns:
            Headers: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    def on_request_send(self, fn: EventHook) -> None:
        """Register a callback function to be called when a request is sent.

        Args:
            fn (EventHook): The callback function to be registered. This function
                            will be called with the request details when a request
                            is sent.

        """
        self._event_hooks["request"].append(fn)

    async def send(self, message: Any) -> bytes:
        """Send a message asynchronously and returns the marshaled data.

        Args:
            message (Any): The message to be sent.

        Returns:
            bytes: The marshaled data of the message.

        Raises:
            Exception: If the response validation fails.

        """
        data = self.marshaler.marshal(message)

        if self.marshaler.enable_get:
            assert self.marshaler.url is not None

            request = httpcore.Request(
                method=HTTPMethod.GET,
                url=httpcore.URL(
                    scheme=self.marshaler.url.scheme,
                    host=self.marshaler.url.host or "",
                    port=self.marshaler.url.port,
                    target=self.marshaler.url.raw_path_qs,
                ),
                headers=list(
                    include_request_headers(
                        headers=self._request_headers, url=self.url, content=data, method=HTTPMethod.GET
                    ).items()
                ),
            )
        else:
            self._request_headers[HEADER_CONTENT_LENGTH] = str(len(data))

            request = httpcore.Request(
                method=HTTPMethod.POST,
                url=httpcore.URL(
                    scheme=self.url.scheme,
                    host=self.url.host or "",
                    port=self.url.port,
                    target=self.url.raw_path,
                ),
                headers=list(
                    include_request_headers(
                        headers=self._request_headers, url=self.url, content=data, method=HTTPMethod.POST
                    ).items()
                ),
                content=data,
            )

        for hook in self._event_hooks["request"]:
            hook(request)

        with map_httpcore_exceptions():
            response = await self.session.pool.handle_async_request(request=request)

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = ResponseAsyncByteStream(response.stream)

        await self._validate_response(response)

        return data

    @property
    def response_headers(self) -> Headers:
        """Return the response headers.

        Returns:
            Headers: A dictionary-like object containing the response headers.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Return the response trailers.

        Response trailers are additional headers sent after the response body.

        Returns:
            Headers: A dictionary containing the response trailers.

        """
        return self._response_trailers

    async def _validate_response(self, response: httpcore.Response) -> None:
        self._response_headers.update(Headers(response.headers))

        for key, value in self._response_headers.items():
            if not key.startswith(CONNECT_UNARY_TRAILER_PREFIX):
                self._response_headers[key] = value
                continue

            self._response_trailers[key[len(CONNECT_UNARY_TRAILER_PREFIX) :]] = value

        connect_validate_unary_response_content_type(
            self.marshaler.connect_marshaler.codec.name,
            response.status,
            self._response_headers.get(HEADER_CONTENT_TYPE, ""),
        )

        compression = self._response_headers.get(CONNECT_UNARY_HEADER_COMPRESSION, None)
        if (
            compression
            and compression != COMPRESSION_IDENTITY
            and not any(c.name == compression for c in self.compressions)
        ):
            raise ConnectError(
                f"unknown encoding {compression}: accepted encodings are {', '.join(c.name for c in self.compressions)}",
                Code.INTERNAL,
            )

        self.unmarshaler.compression = get_compresion_from_name(compression, self.compressions)

        if response.status != HTTPStatus.OK:

            def json_ummarshal(data: bytes, _message: Any) -> Any:
                return json.loads(data)

            try:
                data = await self.unmarshaler.unmarshal_func(None, json_ummarshal)
                wire_error = error_from_json(data)
            except Exception as e:
                raise ConnectError(
                    f"HTTP {response.status}",
                    code_from_http_status(response.status),
                ) from e

            if wire_error.code == 0:
                wire_error.code = code_from_http_status(response.status)

            wire_error.metadata = self._response_headers.copy()
            wire_error.metadata.update(self._response_trailers)
            raise wire_error

        return

    @property
    def event_hooks(self) -> dict[str, list[EventHook]]:
        """Return the event hooks.

        This method returns a dictionary where the keys are strings representing
        event names, and the values are lists of EventHook objects associated with
        those events.

        Returns:
            dict[str, list[EventHook]]: A dictionary mapping event names to lists
            of EventHook objects.

        """
        return self._event_hooks

    @event_hooks.setter
    def event_hooks(self, event_hooks: dict[str, list[EventHook]]) -> None:
        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }


def connect_validate_unary_response_content_type(
    request_codec_name: str,
    status_code: int,
    response_content_type: str,
) -> None:
    """Validate the content type of a unary response based on the HTTP status code and method.

    Args:
        request_codec_name (str): The name of the codec used for the request.
        http_method (HTTPMethod): The HTTP method used for the request.
        status_code (int): The HTTP status code of the response.
        response_content_type (str): The content type of the response.

    Raises:
        ConnectError: If the status code is not OK and the response content type is not valid.

    """
    if status_code != HTTPStatus.OK:
        # Error response must be JSON-encoded.
        if (
            response_content_type == CONNECT_UNARY_CONTENT_TYPE_PREFIX + CodecNameType.JSON
            or response_content_type == CONNECT_UNARY_CONTENT_TYPE_PREFIX + CodecNameType.JSON_CHARSET_UTF8
        ):
            return

        raise ConnectError(
            f"HTTP {status_code}",
            code_from_http_status(status_code),
        )

    if not response_content_type.startswith(CONNECT_UNARY_CONTENT_TYPE_PREFIX):
        raise ConnectError(
            f"invalid content-type: {response_content_type}; expecting {CONNECT_UNARY_CONTENT_TYPE_PREFIX}",
            Code.UNKNOWN,
        )

    response_codec_name = connect_codec_from_content_type(StreamType.Unary, response_content_type)
    if response_codec_name == request_codec_name:
        return

    if (response_codec_name == CodecNameType.JSON and request_codec_name == CodecNameType.JSON_CHARSET_UTF8) or (
        response_codec_name == CodecNameType.JSON_CHARSET_UTF8 and request_codec_name == CodecNameType.JSON
    ):
        return

    raise ConnectError(
        f"invalid content-type: {response_content_type}; expecting {CONNECT_UNARY_CONTENT_TYPE_PREFIX}{request_codec_name}",
        Code.INTERNAL,
    )


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
    match HTTPMethod(request.method):
        case HTTPMethod.GET:
            version = request.query_params.get(CONNECT_UNARY_CONNECT_QUERY_PARAMETER)
            if required and version is None:
                raise ConnectError(
                    f'missing required parameter: set {CONNECT_UNARY_CONNECT_QUERY_PARAMETER} to "{CONNECT_UNARY_CONNECT_QUERY_VALUE}"'
                )
            elif version is not None and version != CONNECT_UNARY_CONNECT_QUERY_VALUE:
                raise ConnectError(
                    f'{CONNECT_UNARY_CONNECT_QUERY_PARAMETER} must be "{CONNECT_UNARY_CONNECT_QUERY_VALUE}": get "{version}"',
                )
        case HTTPMethod.POST:
            version = request.headers.get(CONNECT_HEADER_PROTOCOL_VERSION, None)
            if required and version is None:
                raise ConnectError(
                    f'missing required header: set {CONNECT_HEADER_PROTOCOL_VERSION} to "{CONNECT_PROTOCOL_VERSION}"',
                    Code.INVALID_ARGUMENT,
                )
            elif version is not None and version != CONNECT_PROTOCOL_VERSION:
                raise ConnectError(
                    f'{CONNECT_HEADER_PROTOCOL_VERSION} must be "{CONNECT_PROTOCOL_VERSION}": get "{version}"',
                    Code.INVALID_ARGUMENT,
                )
        case _:
            raise ConnectError(f"unsupported method: {request.method}", Code.INVALID_ARGUMENT)


def connect_code_to_http(code: Code) -> int:
    """Convert a given `Code` enumeration to its corresponding HTTP status code.

    Args:
        code (Code): The `Code` enumeration value to be converted.

    Returns:
        int: The corresponding HTTP status code.

    The mapping is as follows:
        - Code.CANCELED -> 499
        - Code.UNKNOWN -> 500
        - Code.INVALID_ARGUMENT -> 400
        - Code.DEADLINE_EXCEEDED -> 504
        - Code.NOT_FOUND -> 404
        - Code.ALREADY_EXISTS -> 409
        - Code.PERMISSION_DENIED -> 403
        - Code.RESOURCE_EXHAUSTED -> 429
        - Code.FAILED_PRECONDITION -> 400
        - Code.ABORTED -> 409
        - Code.OUT_OF_RANGE -> 400
        - Code.UNIMPLEMENTED -> 501
        - Code.INTERNAL -> 500
        - Code.UNAVAILABLE -> 503
        - Code.DATA_LOSS -> 500
        - Code.UNAUTHENTICATED -> 401
        - Any other code -> 500

    """
    match code:
        case Code.CANCELED:
            return 499
        case Code.UNKNOWN:
            return 500
        case Code.INVALID_ARGUMENT:
            return 400
        case Code.DEADLINE_EXCEEDED:
            return 504
        case Code.NOT_FOUND:
            return 404
        case Code.ALREADY_EXISTS:
            return 409
        case Code.PERMISSION_DENIED:
            return 403
        case Code.RESOURCE_EXHAUSTED:
            return 429
        case Code.FAILED_PRECONDITION:
            return 400
        case Code.ABORTED:
            return 409
        case Code.OUT_OF_RANGE:
            return 400
        case Code.UNIMPLEMENTED:
            return 501
        case Code.INTERNAL:
            return 500
        case Code.UNAVAILABLE:
            return 503
        case Code.DATA_LOSS:
            return 500
        case Code.UNAUTHENTICATED:
            return 401
        case _:
            return 500


def error_from_json_bytes(data: bytes) -> ConnectError:
    """Deserialize a ConnectError object from a JSON-encoded byte string.

    Args:
        data (bytes): The JSON-encoded byte string to deserialize.

    Returns:
        ConnectError: The deserialized ConnectError object.

    Raises:
        ConnectError: If deserialization fails, a ConnectError is raised with an
                      appropriate error message and code.

    """
    try:
        obj = json.loads(data)
    except Exception as e:
        raise Exception(f"failed to parse JSON: {str(e)}") from e

    return error_from_json(obj)


def error_from_json(obj: dict[str, Any]) -> ConnectError:
    """Convert a JSON-serializable dictionary to a ConnectError object.

    Args:
        obj (dict[str, Any]): The dictionary representing the error in JSON format.

    Returns:
        ConnectError: The ConnectError object converted from the dictionary.

    Raises:
        ConnectError: If the dictionary is missing required fields or contains invalid values,
                      a ConnectError is raised with an appropriate error message and code.

    """
    code = obj.get("code")
    if code is None:
        raise Exception("missing required field: code")

    message = obj.get("message", "")
    details = obj.get("details", [])

    error = ConnectError(message, Code.from_string(code), wire_error=True)

    for detail in details:
        type_name = detail.get("type", None)
        value = detail.get("value", None)

        if type_name is None:
            raise Exception("missing required field: type")
        if value is None:
            raise Exception("missing required field: value")

        type_name = type_name if "/" in type_name else DEFAULT_ANY_RESOLVER_PREFIX + type_name
        try:
            decoded = base64.b64decode(value.encode() + b"=" * (4 - len(value) % 4))
        except Exception as e:
            message = str(e)
            raise Exception(f"failed to decode value: {message}") from e

        error.details.append(
            ErrorDetail(pb_any=any_pb2.Any(type_url=type_name, value=decoded), wire_json=json.dumps(detail))
        )

    return error


def end_stream_from_bytes(data: bytes) -> tuple[ConnectError | None, Headers]:
    """Parse a byte stream to extract metadata and error information.

    Args:
        data (bytes): The byte stream to be parsed.

    Returns:
        tuple[ConnectError | None, Headers]: A tuple containing an optional ConnectError
        and a Headers object with the parsed metadata.

    Raises:
        ConnectError: If the byte stream is invalid or the metadata format is incorrect.

    """
    try:
        obj = json.loads(data)
    except Exception as e:
        raise ConnectError(
            "invalid end stream",
            Code.UNKNOWN,
        ) from e

    metadata = Headers()
    if "metadata" in obj:
        if not isinstance(obj["metadata"], dict) or not all(
            isinstance(k, str) and isinstance(v, list) for k, v in obj["metadata"].items()
        ):
            raise ConnectError(
                "invalid end stream",
                Code.UNKNOWN,
            )

        for key, values in obj["metadata"].items():
            value = ", ".join(values)
            metadata[key] = value

    if "error" in obj:
        error = error_from_json(obj["error"])
        return error, metadata
    else:
        return None, metadata


def error_to_json(error: ConnectError) -> dict[str, Any]:
    """Convert a ConnectError object to a JSON-serializable dictionary.

    Args:
        error (ConnectError): The error object to convert.

    Returns:
        dict[str, Any]: A dictionary representing the error in JSON format.
            - "code" (str): The error code as a string.
            - "message" (str, optional): The raw error message, if available.
            - "details" (list[dict[str, Any]], optional): A list of dictionaries containing error details, if available.
                Each detail dictionary contains:
                - "type" (str): The type name of the detail.
                - "value" (str): The base64-encoded value of the detail.
                - "debug" (str, optional): The JSON-encoded debug information, if available.

    """
    obj: dict[str, Any] = {"code": error.code.string()}

    if len(error.raw_message) > 0:
        obj["message"] = error.raw_message

    if len(error.details) > 0:
        wires = []
        for detail in error.details:
            wire: dict[str, Any] = {
                "type": detail.pb_any.TypeName(),
                "value": base64.b64encode(detail.pb_any.value).decode("utf-8").rstrip("="),
            }

            with contextlib.suppress(Exception):
                meg = detail.get_inner()
                wire["debug"] = json_format.MessageToDict(meg)

            wires.append(wire)

        obj["details"] = wires

    return obj


def error_to_json_bytes(error: ConnectError) -> bytes:
    """Serialize a ConnectError object to a JSON-encoded byte string.

    Args:
        error (ConnectError): The ConnectError object to serialize.

    Returns:
        bytes: The JSON-encoded byte string representation of the error.

    Raises:
        ConnectError: If serialization fails, a ConnectError is raised with an
                      appropriate error message and code.

    """
    try:
        json_obj = error_to_json(error)
        json_str = json.dumps(json_obj)

        return json_str.encode("utf-8")
    except Exception as e:
        message = str(e)
        raise ConnectError(f"failed to serialize Connect Error: {message}", Code.INTERNAL) from e
