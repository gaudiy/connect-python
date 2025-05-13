"""gRPC and gRPC-Web protocol handler implementation for Connect Python server."""

from collections.abc import AsyncIterable, AsyncIterator
from http import HTTPMethod
from typing import Any

from connect.code import Code
from connect.compression import COMPRESSION_IDENTITY
from connect.connect import (
    Address,
    Peer,
    Spec,
    StreamingHandlerConn,
)
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    PROTOCOL_GRPC,
    ProtocolHandler,
    ProtocolHandlerParams,
    negotiate_compression,
)
from connect.protocol_grpc.constants import (
    GRPC_ALLOWED_METHODS,
    GRPC_HEADER_ACCEPT_COMPRESSION,
    GRPC_HEADER_COMPRESSION,
    GRPC_HEADER_TIMEOUT,
    MAX_HOURS,
    RE_TIMEOUT,
    UNIT_TO_SECONDS,
)
from connect.protocol_grpc.content_type import grpc_codec_from_content_type
from connect.protocol_grpc.error_trailer import grpc_error_to_trailer
from connect.protocol_grpc.marshaler import GRPCMarshaler
from connect.protocol_grpc.unmarshaler import GRPCUnmarshaler
from connect.request import Request
from connect.response import StreamingResponse
from connect.response_writer import ServerResponseWriter
from connect.utils import aiterate


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
            web=self.web,
            writer=writer,
            spec=self.params.spec,
            peer=peer,
            marshaler=GRPCMarshaler(
                codec,
                response_compression,
                self.params.compress_min_bytes,
                self.params.send_max_bytes,
            ),
            unmarshaler=GRPCUnmarshaler(
                self.web,
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

    web: bool
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
        web: bool,
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
            web (bool): Indicates if the connection is for a web environment.
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
        self.web = web
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

        m = RE_TIMEOUT.match(timeout)
        if m is None:
            raise ConnectError(f"protocol error: invalid grpc timeout value: {timeout}")

        num_str, unit = m.groups()
        num = int(num_str)
        if num > 99_999_999:
            raise ConnectError(f"protocol error: timeout {timeout!r} is too long")

        if unit == "H" and num > MAX_HOURS:
            return None

        seconds = num * UNIT_TO_SECONDS[unit]
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

    async def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and processes it.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            AsyncIterator[Any]: An async iterator yielding message(s). For non-streaming operations,
                             this will yield exactly one item.

        """
        async for obj, _ in self.unmarshaler.unmarshal(message):
            yield obj

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
        if self.web:
            await self.writer.write(
                StreamingResponse(
                    content=self._send_messages(messages),
                    headers=self.response_headers,
                    status_code=200,
                )
            )
        else:
            await self.writer.write(
                StreamingResponse(
                    content=self._send_messages(messages),
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

    async def _send_messages(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Asynchronously sends marshaled messages and yields them as byte streams.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled and sent.

        Yields:
            bytes: Marshaled message bytes, and optionally marshaled web trailers if in web mode.

        Raises:
            ConnectError: If an error occurs during marshaling or sending messages, a ConnectError is set and handled.

        Notes:
            - Errors encountered during message marshaling are converted to ConnectError and added to response trailers.
            - If running in web mode (`self.web` is True), marshaled web trailers are yielded at the end.

        """
        error: ConnectError | None = None
        try:
            async for msg in self.marshaler.marshal(messages):
                yield msg
        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
        finally:
            grpc_error_to_trailer(self.response_trailers, error)

            if self.web:
                body = await self.marshaler.marshal_web_trailers(self.response_trailers)
                yield body

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
        if self.web:
            body = await self.marshaler.marshal_web_trailers(self.response_trailers)

            await self.writer.write(
                StreamingResponse(
                    content=aiterate([body]),
                    headers=self.response_headers,
                    status_code=200,
                )
            )
        else:
            await self.writer.write(
                StreamingResponse(
                    content=[],
                    headers=self.response_headers,
                    trailers=self.response_trailers,
                    status_code=200,
                )
            )
