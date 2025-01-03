"""Provides classes and functions for handling protocol connections.

Handles data serialization/deserialization using the Connect protocol.
"""

import base64
import contextlib
import json
from typing import Any

from starlette.datastructures import MutableHeaders

from connect.code import Code
from connect.codec import Codec, CodecNameType, ProtoJSONCodec
from connect.compression import Compression
from connect.connect import Spec, StreamingHandlerConn, StreamType
from connect.error import ConnectError
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
CONNECT_UNARY_TRAILER_PREFIX = "trailer-"
CONNECT_HEADER_PROTOCOL_VERSION = "connect-protocol-version"
CONNECT_PROTOCOL_VERSION = "1"

CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"
CONNECT_UNARY_CONTENT_TYPE_JSON = "application/json"

CONNECT_UNARY_ENCODING_QUERY_PARAMETER = "encoding"
CONNECT_UNARY_MESSAGE_QUERY_PARAMETER = "message"
CONNECT_UNARY_BASE64_QUERY_PARAMETER = "base64"
CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"
CONNECT_UNARY_CONNECT_QUERY_PARAMETER = "connect"
CONNECT_UNARY_CONNECT_QUERY_VALUE = "v" + CONNECT_PROTOCOL_VERSION


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

    def content_types(self) -> list[str]:
        """Handle content types.

        This method currently does nothing and serves as a placeholder for future
        implementation related to content types.

        """
        return self.accept

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Check if the handler can handle the payload."""
        if HttpMethod(request.method) == HttpMethod.GET:
            codec_name = request.query_params.get(CONNECT_UNARY_ENCODING_QUERY_PARAMETER, "")
            content_type = connect_content_type_from_codec_name(self.params.spec.stream_type, codec_name)

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
        query_params = request.query_params

        if self.params.spec.stream_type == StreamType.Unary:
            if HttpMethod(request.method) == HttpMethod.GET:
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

        request_body: bytes

        if HttpMethod(request.method) == HttpMethod.GET:
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
                decoded = base64.b64decode(message)
            else:
                decoded = message.encode("utf-8")

            request_body = decoded
            codec_name = encoding
            content_type = connect_content_type_from_codec_name(self.params.spec.stream_type, codec_name)

        else:
            request_body = await request.body()
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
        response_headers[accept_compression_header] = f"{', '.join(c.name() for c in self.params.compressions)}"

        if self.params.spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryHandlerConn(
                request=request,
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
            # TODO(tsubakiky): Check if idempotency level is NoSideEffect.
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
            # TODO(tsubakiky): Add error handling for decompression
            data = self.compression.decompress(data, self.read_max_bytes)

        try:
            # TODO(tsubakiky): Add error handling for unmarshal
            obj = self.codec.unmarshal(data, message)
        except Exception as e:
            raise ConnectError(
                f"unmarshal message: {str(e)}",
                Code.INVALID_ARGUMENT,
            ) from e

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
        try:
            # TODO(tsubakiky): Add error handling for marshal
            data = self.codec.marshal(message)
        except Exception as e:
            raise ConnectError(f"marshal message: {str(e)}", Code.INTERNAL) from e

        if len(data) < self.compress_min_bytes or self.compression is None:
            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                raise ConnectError(
                    f"message size {len(data)} exceeds send_mas_bytes {self.send_max_bytes}", Code.RESOURCE_EXHAUSTED
                )

            return data

        # TODO(tsubakiky): Add error handling for compression
        data = self.compression.compress(data)

        if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
            raise ConnectError(
                f"compressed message size {len(data)} exceeds send_mas_bytes {self.send_max_bytes}",
                Code.RESOURCE_EXHAUSTED,
            )

        self.response_headers[CONNECT_UNARY_HEADER_COMPRESSION] = self.compression.name()

        return data


class ConnectUnaryHandlerConn(StreamingHandlerConn):
    """ConnectUnaryHandlerConn is a handler connection class for unary RPCs in the Connect protocol.

    Attributes:
        request (Request): The incoming request object.
        marshaler (ConnectMarshaler): An instance of ConnectMarshaler used to marshal messages.
        unmarshaler (ConnectUnmarshaler): An instance of ConnectUnmarshaler used to unmarshal messages.
        response_headers (MutableHeaders): The headers for the response.

    """

    request: Request
    marshaler: ConnectMarshaler
    unmarshaler: ConnectUnmarshaler
    response_headers: MutableHeaders
    response_trailers: MutableHeaders

    def __init__(
        self,
        request: Request,
        marshaler: ConnectMarshaler,
        unmarshaler: ConnectUnmarshaler,
        response_headers: MutableHeaders,
        response_trailers: MutableHeaders | None = None,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            request (Request): The incoming request object.
            marshaler (ConnectMarshaler): The marshaler to serialize data.
            unmarshaler (ConnectUnmarshaler): The unmarshaler to deserialize data.
            response_headers (MutableHeaders): The headers for the response.
            response_trailers (MutableHeaders, optional): The trailers for the response.

        """
        self.request = request
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self.response_headers = response_headers
        self.response_trailers = response_trailers or MutableHeaders()

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

    def response_header(self) -> MutableHeaders:
        """Retrieve the response header.

        Returns:
            Any: The response header.

        """
        return self.response_headers

    def response_trailer(self) -> MutableHeaders:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The processed response trailer data.

        """
        return self.response_trailers

    def close(self, data: bytes) -> Response:
        """Close the connection and returns a response with the provided data.

        Args:
            data (bytes): The data to include in the response.

        Returns:
            Response: A response object containing the provided data, response headers, and a status code of 200.

        """
        self.merge_response_header()
        response = Response(content=data, headers=self.response_headers, status_code=200)
        return response

    def close_with_error(self, e: Exception) -> Response:
        """Close the connection with an error response.

        Args:
            e (Exception): The exception that caused the error.

        Returns:
            Response: The HTTP response containing the error details.

        """
        error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
        status = connect_code_to_http(error.code)

        self.response_headers[HEADER_CONTENT_TYPE] = CONNECT_UNARY_CONTENT_TYPE_JSON
        self.response_headers.update(error.metadata)

        body = error_to_json_bytes(error)

        self.merge_response_header()
        return Response(content=body, headers=self.response_headers, status_code=status)

    def merge_response_header(self) -> None:
        """Merge response headers into response trailers with a specific prefix.

        This method iterates over the items in `self.response_headers` and adds each
        header to `self.response_trailers` with a prefix defined by `CONNECT_UNARY_TRAILER_PREFIX`.

        Returns:
            None

        """
        for key, value in self.response_headers.items():
            self.response_trailers[CONNECT_UNARY_TRAILER_PREFIX + key] = value


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
            version = request.query_params.get(CONNECT_UNARY_CONNECT_QUERY_PARAMETER)
            if required and version is None:
                raise ConnectError(
                    f'missing required parameter: set {CONNECT_UNARY_CONNECT_QUERY_PARAMETER} to "{CONNECT_UNARY_CONNECT_QUERY_VALUE}"'
                )
            elif version is not None and version != CONNECT_UNARY_CONNECT_QUERY_VALUE:
                raise ConnectError(
                    f'{CONNECT_UNARY_CONNECT_QUERY_PARAMETER} must be "{CONNECT_UNARY_CONNECT_QUERY_VALUE}": get "{version}"',
                )
        case HttpMethod.POST:
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

            codec = ProtoJSONCodec(CodecNameType.JSON)

            with contextlib.suppress(Exception):
                wire["debug"] = codec.marshal(wire)

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
