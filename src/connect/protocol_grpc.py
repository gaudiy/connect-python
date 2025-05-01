"""Provaides classes and functions for handling gRPC protocol."""

import base64
import re
import urllib.parse
from collections.abc import AsyncIterable, AsyncIterator
from http import HTTPMethod
from typing import Any

from google.rpc import status_pb2

from connect.code import Code
from connect.codec import Codec, CodecNameType
from connect.compression import COMPRESSION_IDENTITY, Compression
from connect.connect import Address, AsyncContentStream, Peer, Spec, StreamingHandlerConn
from connect.envelope import EnvelopeReader, EnvelopeWriter
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    PROTOCOL_GRPC,
    Protocol,
    ProtocolClient,
    ProtocolClientParams,
    ProtocolHandler,
    ProtocolHandlerParams,
    exclude_protocol_headers,
    negotiate_compression,
)
from connect.request import Request
from connect.response_trailer import StreamingResponseWithTrailers
from connect.utils import aiterate
from connect.writer import ServerResponseWriter

GRPC_HEADER_COMPRESSION = "Grpc-Encoding"
GRPC_HEADER_ACCEPT_COMPRESSION = "Grpc-Accept-Encoding"
GRPC_HEADER_TIMEOUT = "Grpc-Timeout"
GRPC_HEADER_STATUS = "Grpc-Status"
GRPC_HEADER_MESSAGE = "Grpc-Message"
GRPC_HEADER_DETAILS = "Grpc-Status-Details-Bin"

GRPC_CONTENT_TYPE_DEFAULT = "application/grpc"
GRPC_WEB_CONTENT_TYPE_DEFAULT = "application/grpc-web"
GRPC_CONTENT_TYPE_PREFIX = GRPC_CONTENT_TYPE_DEFAULT + "+"
GRPC_WEB_CONTENT_TYPE_PREFIX = GRPC_WEB_CONTENT_TYPE_DEFAULT + "+"

GRPC_ALLOWED_METHODS = [HTTPMethod.POST]


_RE = re.compile(r"^(\d{1,8})([HMSmun])$")
_UNIT_TO_SECONDS = {
    "H": 60 * 60,
    "M": 60,
    "S": 1,
    "m": 1e-3,  # millisecond
    "u": 1e-6,  # microsecond
    "n": 1e-9,  # nanosecond
}
_MAX_HOURS = (2**63 - 1) // (60 * 60 * 1_000_000_000)


class ProtocolGPRC(Protocol):
    """ProtocolGPRC is a protocol implementation for handling gRPC and gRPC-Web requests.

    Attributes:
        web (bool): Indicates whether to use gRPC-Web (True) or standard gRPC (False).

    """

    def __init__(self, web: bool) -> None:
        """Initialize the instance.

        Args:
            web (bool): Indicates whether the instance is for web usage.

        """
        self.web = web

    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Create and returns a GRPCHandler instance configured with appropriate content types based on the provided parameters.

        Args:
            params (ProtocolHandlerParams): The parameters containing codec information and other handler configuration.

        Returns:
            ProtocolHandler: An instance of GRPCHandler initialized with the correct content types for gRPC or gRPC-Web.

        Behavior:
            - Determines the default and prefix content types based on whether gRPC-Web is enabled.
            - Constructs a list of supported content types from the available codecs.
            - Adds the bare content type if the PROTO codec is present.
            - Returns a GRPCHandler with the computed content types.

        """
        bare, prefix = GRPC_CONTENT_TYPE_DEFAULT, GRPC_CONTENT_TYPE_PREFIX
        if self.web:
            bare, prefix = GRPC_WEB_CONTENT_TYPE_DEFAULT, GRPC_WEB_CONTENT_TYPE_PREFIX

        content_types: list[str] = []
        for name in params.codecs.names():
            content_types.append(prefix + name)

        if params.codecs.get(CodecNameType.PROTO):
            content_types.append(bare)

        return GRPCHandler(params, self.web, content_types)

    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Create and return a GRPCClient instance.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the client.

        Returns:
            ProtocolClient: An instance of GRPCClient.

        """
        raise NotImplementedError("GRPC client is not implemented yet.")


class GRPCHandler(ProtocolHandler):
    """GRPCHandler is a protocol handler for gRPC and gRPC-Web requests.

    This class implements the ProtocolHandler interface to handle gRPC protocol requests,
    including negotiation of compression, codec selection, and connection management for
    both standard gRPC and gRPC-Web. It supports content type negotiation, payload handling,
    and manages the lifecycle of a gRPC connection, including streaming and non-streaming
    requests.

    Attributes:
        params (ProtocolHandlerParams): Configuration parameters for the handler, including codecs and compressions.
        web (bool): Indicates if the handler is for gRPC-Web.
        accept (list[str]): List of accepted content types.

    """

    params: ProtocolHandlerParams
    web: bool
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, web: bool, accept: list[str]) -> None:
        """Initialize the ProtocolHandler with the given parameters.

        Args:
            params (ProtocolHandlerParams): The parameters required for the protocol handler.
            web (bool): Indicates whether the handler is for web usage.
            accept (list[str]): A list of accepted content types.

        Returns:
            None

        """
        self.params = params
        self.web = web
        self.accept = accept

    @property
    def methods(self) -> list[HTTPMethod]:
        """Returns a list of allowed HTTP methods for gRPC protocol.

        Returns:
            list[HTTPMethod]: A list containing the HTTP methods permitted for gRPC communication.

        """
        return GRPC_ALLOWED_METHODS

    def content_types(self) -> list[str]:
        """Return a list of accepted content types.

        Returns:
            list[str]: A list of MIME types that are accepted.

        """
        return self.accept

    def can_handle_payload(self, _: Request, content_type: str) -> bool:
        """Determine if the given content type is supported by this handler.

        Args:
            _ (Request): The request object (unused).
            content_type (str): The MIME type of the payload to check.

        Returns:
            bool: True if the content type is accepted, False otherwise.

        """
        return content_type in self.accept

    async def conn(
        self,
        request: Request,
        response_headers: Headers,
        response_trailers: Headers,
        writer: ServerResponseWriter,
        is_streaming: bool = False,
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

        """
        content_encoding = request.headers.get(GRPC_HEADER_COMPRESSION)
        accept_encoding = request.headers.get(GRPC_HEADER_ACCEPT_COMPRESSION)

        request_compression, response_compression, error = negotiate_compression(
            self.params.compressions, content_encoding, accept_encoding
        )

        response_headers[HEADER_CONTENT_TYPE] = request.headers.get(HEADER_CONTENT_TYPE, "")
        response_headers[GRPC_HEADER_ACCEPT_COMPRESSION] = f"{', '.join(c.name for c in self.params.compressions)}"
        if response_compression and response_compression.name != COMPRESSION_IDENTITY:
            response_headers[GRPC_HEADER_COMPRESSION] = response_compression.name

        codec_name = grpc_codec_from_content_type(self.web, request.headers.get(HEADER_CONTENT_TYPE, ""))
        codec = self.params.codecs.get(codec_name)
        protocol_name = PROTOCOL_GRPC if not self.web else PROTOCOL_GRPC + "-web"

        peer = Peer(
            address=Address(host=request.client.host, port=request.client.port) if request.client else request.client,
            protocol=protocol_name,
            query=request.query_params,
        )

        conn = GRPCHandlerConn(
            writer=writer,
            spec=self.params.spec,
            peer=peer,
            marshaler=GRPCMarshaler(
                self.web,
                codec,
                response_compression,
                self.params.compress_min_bytes,
                self.params.send_max_bytes,
            ),
            unmarshaler=GRPCUnmarshaler(
                codec,
                self.params.read_max_bytes,
                request.stream(),
                request_compression,
            ),
            request_headers=Headers(request.headers, encoding="latin-1"),
            response_headers=response_headers,
            response_trailers=response_trailers,
        )

        if error:
            await conn.send_error(error)
            return None

        return conn


class GRPCMarshaler(EnvelopeWriter):
    """GRPCMarshaler is responsible for marshaling messages into the gRPC wire format.

    Args:
        web (bool): Indicates whether to use the gRPC-web protocol.
        codec (Codec | None): The codec used for encoding/decoding messages.
        compression (Compression | None): The compression algorithm to use, if any.
        compress_min_bytes (int): Minimum message size in bytes before compression is applied.
        send_max_bytes (int): Maximum allowed size of a message to send.

    Methods:
        marshal(messages: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
            Asynchronously marshals a stream of message bytes into the gRPC wire format.
            Yields marshaled message bytes ready for transmission.

    """

    web: bool

    def __init__(
        self,
        web: bool,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
    ) -> None:
        """Initialize the protocol with the specified configuration.

        Args:
            web (bool): Indicates whether the protocol is used in a web context.
            codec (Codec | None): The codec to use for encoding/decoding messages, or None for default.
            compression (Compression | None): The compression algorithm to use, or None for no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes allowed to send in a single message.

        Returns:
            None

        """
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes)
        self.web = web


class GRPCUnmarshaler(EnvelopeReader):
    """GRPCUnmarshaler is a specialized EnvelopeReader for handling gRPC message unmarshaling.

    Args:
        codec (Codec | None): The codec used for decoding messages.
        read_max_bytes (int): The maximum number of bytes to read from the stream.
        stream (AsyncIterable[bytes] | None, optional): The asynchronous byte stream to read messages from.
        compression (Compression | None, optional): Compression algorithm to use for decompressing messages.

    Methods:
        async unmarshal(message: Any) -> AsyncIterator[Any]:
            Asynchronously unmarshals the given message, yielding each decoded object.
            Iterates over the results of the internal _unmarshal method, yielding only the object part of each tuple.

    """

    def __init__(
        self,
        codec: Codec | None,
        read_max_bytes: int,
        stream: AsyncIterable[bytes] | None = None,
        compression: Compression | None = None,
    ) -> None:
        """Initialize the protocol gRPC handler.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding messages. Can be None.
            read_max_bytes (int): The maximum number of bytes to read from the stream.
            stream (AsyncIterable[bytes] | None, optional): An asynchronous iterable stream of bytes. Defaults to None.
            compression (Compression | None, optional): The compression method to use. Defaults to None.

        """
        super().__init__(codec, read_max_bytes, stream, compression)

    async def unmarshal(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously unmarshals a given message and yields each resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Yields:
            Any: Each object obtained from unmarshaling the message.

        """
        async for obj, _ in super().unmarshal(message):
            yield obj


class GRPCHandlerConn(StreamingHandlerConn):
    """GRPCHandlerConn is a handler class for managing gRPC protocol connections within a streaming server context.

    This class encapsulates the logic for handling gRPC requests and responses, including marshaling and unmarshaling messages,
    managing request and response headers/trailers, handling timeouts, and enforcing protocol-specific constraints for unary and streaming operations.

    Attributes:
        _spec (Spec): The specification object describing the protocol or service.
        _peer (Peer): The peer information for the current connection.
        _request_headers (Headers): The headers received with the request.
        _response_headers (Headers): The headers to include in the response.
        _response_trailers (Headers): The trailers to include in the response.
        _is_streaming (bool): Indicates if the connection is streaming.

    """

    _spec: Spec
    _peer: Peer
    writer: ServerResponseWriter
    marshaler: GRPCMarshaler
    unmarshaler: GRPCUnmarshaler
    _request_headers: Headers
    _response_headers: Headers
    _response_trailers: Headers

    def __init__(
        self,
        writer: ServerResponseWriter,
        spec: Spec,
        peer: Peer,
        marshaler: GRPCMarshaler,
        unmarshaler: GRPCUnmarshaler,
        request_headers: Headers,
        response_headers: Headers,
        response_trailers: Headers | None = None,
    ) -> None:
        """Initialize a new instance of the class.

        Args:
            writer (ServerResponseWriter): The writer used to send responses to the client.
            spec (Spec): The specification object describing the protocol or service.
            peer (Peer): The peer information for the current connection.
            marshaler (GRPCMarshaler): The marshaler used to serialize response messages.
            unmarshaler (GRPCUnmarshaler): The unmarshaler used to deserialize request messages.
            request_headers (Headers): The headers received with the request.
            response_headers (Headers): The headers to include in the response.
            response_trailers (Headers | None, optional): The trailers to include in the response. Defaults to None.
            is_streaming (bool, optional): Indicates if the connection is streaming. Defaults to False.

        """
        self.writer = writer
        self._spec = spec
        self._peer = peer
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers if response_trailers is not None else Headers()

    def parse_timeout(self) -> float | None:
        """Parse the gRPC timeout value from the request headers and returns it as seconds.

        Returns:
            float | None: The timeout value in seconds if present and valid, otherwise None.

        Raises:
            ConnectError: If the timeout value is present but invalid or too long.

        Notes:
            - The timeout is extracted from the gRPC header and must match the expected format.
            - If the timeout unit is hours and exceeds the maximum allowed, None is returned.

        """
        timeout = self._request_headers.get(GRPC_HEADER_TIMEOUT)
        if not timeout:
            return None

        m = _RE.match(timeout)
        if m is None:
            raise ConnectError(f"protocol error: invalid grpc timeout value: {timeout}")

        num_str, unit = m.groups()
        num = int(num_str)

        if num > 99_999_999:
            raise ConnectError(f"protocol error: timeout {timeout!r} is too long")

        if unit == "H" and num > _MAX_HOURS:
            return None

        seconds = num * _UNIT_TO_SECONDS[unit]
        return seconds

    @property
    def spec(self) -> Spec:
        """Returns the specification object associated with this instance.

        Returns:
            Spec: The specification object.

        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object.

        Returns:
            Peer: The peer instance associated with this object.

        """
        return self._peer

    def receive(self, message: Any) -> AsyncContentStream[Any]:
        """Receives a message and processes it.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            AsyncIterator[Any]: An async iterator yielding message(s). For non-streaming operations,
                             this will yield exactly one item.

        """
        return AsyncContentStream(self.unmarshaler.unmarshal(message), self.spec.stream_type)

    @property
    def request_headers(self) -> Headers:
        """Returns the headers associated with the current request.

        Returns:
            Headers: The headers of the request.

        """
        return self._request_headers

    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Send message(s) by marshaling them into bytes.

        Args:
            messages (AsyncIterable[Any]): The message(s) to be sent. For unary operations,
                                         this should be an iterable with a single item.

        Returns:
            None

        """
        await self.writer.write(
            StreamingResponseWithTrailers(
                content=self.marshal_with_error_handling(messages),
                headers=self.response_headers,
                trailers=self.response_trailers,
                status_code=200,
            )
        )

    @property
    def response_headers(self) -> Headers:
        """Returns the response headers associated with the current request.

        Returns:
            Headers: The headers returned in the response.

        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers as headers.

        Response trailers are additional metadata sent by the server after the response body,
        typically used in gRPC and HTTP/2 protocols.

        Returns:
            Headers: The response trailers associated with the current response.

        """
        return self._response_trailers

    async def marshal_with_error_handling(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Marshal messages to bytes with error handling.

        Args:
            messages (AsyncIterable[Any]): The messages to marshal

        Returns:
            AsyncIterator[bytes]: An async iterator of marshaled bytes

        """
        error: ConnectError | None = None
        try:
            async for msg in self.marshaler.marshal(messages):
                yield msg
        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
        finally:
            grpc_error_to_trailer(self.response_trailers, error)

    async def send_error(self, error: ConnectError) -> None:
        """Send an error response over gRPC by converting the provided ConnectError into gRPC trailers.

        Args:
            error (ConnectError): The error to be sent as a gRPC trailer.

        Returns:
            None

        This method updates the response trailers with the error information and writes a streaming response
        with the appropriate headers and trailers to the client.

        """
        grpc_error_to_trailer(self.response_trailers, error)

        await self.writer.write(
            StreamingResponseWithTrailers(
                content=aiterate([b""]), headers=self.response_headers, trailers=self.response_trailers, status_code=200
            )
        )


def grpc_codec_from_content_type(web: bool, content_type: str) -> str:
    """Determine the gRPC codec name from the given content type string.

    Args:
        web (bool): Indicates whether the request is a gRPC-web request.
        content_type (str): The content type string to parse.

    Returns:
        str: The codec name extracted from the content type. If the content type matches the default gRPC or gRPC-web content type,
             returns the default codec name. Otherwise, extracts and returns the codec name from the content type prefix, or returns
             the original content type if no known prefix is found.

    """
    if (not web and content_type == GRPC_CONTENT_TYPE_DEFAULT) or (
        web and content_type == GRPC_WEB_CONTENT_TYPE_DEFAULT
    ):
        return CodecNameType.PROTO

    prefix = GRPC_CONTENT_TYPE_PREFIX if not web else GRPC_WEB_CONTENT_TYPE_PREFIX

    if content_type.startswith(prefix):
        return content_type[len(prefix) :]
    else:
        return content_type


def grpc_error_to_trailer(trailer: Headers, error: ConnectError | None) -> None:
    """Convert a ConnectError to gRPC trailer headers.

    Args:
        trailer (Headers): The trailer headers dictionary to update with gRPC error information.
        error (ConnectError | None): The error to convert. If None, indicates success.

    Side Effects:
        Modifies the `trailer` dictionary in-place to include gRPC status, message, and optional details.

    Notes:
        - If `error` is None, sets the gRPC status header to "0" (OK).
        - If `ConnectError.wire_error` is False, updates the trailer with error metadata excluding protocol headers.
        - Serializes error details using protobuf if present, encoding them in base64 for the trailer.

    """
    if error is None:
        trailer[GRPC_HEADER_STATUS] = "0"
        return

    if not ConnectError.wire_error:
        trailer.update(exclude_protocol_headers(error.metadata))

    status = status_pb2.Status(
        code=error.code.value,
        message=error.raw_message,
        details=error.details_any(),
    )
    code = status.code
    message = status.message
    bin = None

    if len(status.details) > 0:
        bin = status.SerializeToString()

    trailer[GRPC_HEADER_STATUS] = str(code)
    trailer[GRPC_HEADER_MESSAGE] = urllib.parse.quote(message)
    if bin:
        trailer[GRPC_HEADER_DETAILS] = base64.b64encode(bin).decode().rstrip("=")
