"""Provides a ConnectHander class for handling connection protocols."""

import base64
import json
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
)
from http import HTTPMethod, HTTPStatus
from typing import Any
from urllib.parse import unquote

from connect.code import Code
from connect.compression import COMPRESSION_IDENTITY
from connect.connect import (
    Address,
    Peer,
    Spec,
    StreamingHandlerConn,
    StreamType,
    ensure_single,
)
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    PROTOCOL_CONNECT,
    ProtocolHandler,
    ProtocolHandlerParams,
    exclude_protocol_headers,
    negotiate_compression,
)
from connect.protocol_connect.constants import (
    CONNECT_HEADER_PROTOCOL_VERSION,
    CONNECT_HEADER_TIMEOUT,
    CONNECT_PROTOCOL_VERSION,
    CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
    CONNECT_STREAMING_HEADER_COMPRESSION,
    CONNECT_UNARY_BASE64_QUERY_PARAMETER,
    CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER,
    CONNECT_UNARY_CONNECT_QUERY_PARAMETER,
    CONNECT_UNARY_CONNECT_QUERY_VALUE,
    CONNECT_UNARY_CONTENT_TYPE_JSON,
    CONNECT_UNARY_ENCODING_QUERY_PARAMETER,
    CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION,
    CONNECT_UNARY_HEADER_COMPRESSION,
    CONNECT_UNARY_MESSAGE_QUERY_PARAMETER,
    CONNECT_UNARY_TRAILER_PREFIX,
)
from connect.protocol_connect.content_type import connect_codec_from_content_type, connect_content_type_from_codec_name
from connect.protocol_connect.error_code import connect_code_to_http
from connect.protocol_connect.error_json import error_to_json
from connect.protocol_connect.marshaler import ConnectStreamingMarshaler, ConnectUnaryMarshaler
from connect.protocol_connect.unmarshaler import ConnectStreamingUnmarshaler, ConnectUnaryUnmarshaler
from connect.request import Request
from connect.response import Response, StreamingResponse
from connect.response_writer import ServerResponseWriter
from connect.utils import (
    aiterate,
)


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
            ConnectError: If there is an error in negotiating compression, protocol version, or message encoding.

        """
        query_params = request.query_params

        if self.params.spec.stream_type == StreamType.Unary:
            if HTTPMethod(request.method) == HTTPMethod.GET:
                content_encoding = query_params.get(CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER, None)
            else:
                content_encoding = request.headers.get(CONNECT_UNARY_HEADER_COMPRESSION, None)
            accept_encoding = request.headers.get(CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION, None)
        else:
            content_encoding = request.headers.get(CONNECT_STREAMING_HEADER_COMPRESSION, None)
            accept_encoding = request.headers.get(CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION, None)

        request_compression, response_compression, error = negotiate_compression(
            self.params.compressions, content_encoding, accept_encoding
        )

        if error is None:
            required = self.params.require_connect_protocol_header and self.params.spec.stream_type == StreamType.Unary
            error = connect_check_protocol_version(request, required)

        if HTTPMethod(request.method) == HTTPMethod.GET:
            encoding = query_params.get(CONNECT_UNARY_ENCODING_QUERY_PARAMETER, "")
            message = query_params.get(CONNECT_UNARY_MESSAGE_QUERY_PARAMETER, "")
            if error is None and encoding == "":
                error = ConnectError(
                    f"missing {CONNECT_UNARY_ENCODING_QUERY_PARAMETER} parameter",
                    Code.INVALID_ARGUMENT,
                )
            if error is None and message == "":
                error = ConnectError(
                    f"missing {CONNECT_UNARY_MESSAGE_QUERY_PARAMETER} parameter",
                    Code.INVALID_ARGUMENT,
                )

            if query_params.get(CONNECT_UNARY_BASE64_QUERY_PARAMETER) == "1":
                message_unquoted = unquote(message)
                decoded = base64.urlsafe_b64decode(message_unquoted + "=" * (-len(message_unquoted) % 4))
            else:
                decoded = message.encode()

            request_stream = aiterate([decoded])
            codec_name = encoding
            content_type = connect_content_type_from_codec_name(self.params.spec.stream_type, codec_name)
        else:
            request_stream = request.stream()
            content_type = request.headers.get(HEADER_CONTENT_TYPE, "")
            codec_name = connect_codec_from_content_type(self.params.spec.stream_type, content_type)

        codec = self.params.codecs.get(codec_name)
        if error is None and codec is None:
            error = ConnectError(
                f"invalid message encoding: {codec_name}",
                Code.INVALID_ARGUMENT,
            )

        response_headers[HEADER_CONTENT_TYPE] = content_type

        if self.params.spec.stream_type == StreamType.Unary:
            response_headers[CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION] = (
                f"{', '.join(c.name for c in self.params.compressions)}"
            )
        else:
            if response_compression and response_compression.name != COMPRESSION_IDENTITY:
                response_headers[CONNECT_STREAMING_HEADER_COMPRESSION] = response_compression.name

            response_headers[CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION] = (
                f"{', '.join(c.name for c in self.params.compressions)}"
            )

        peer = Peer(
            address=Address(host=request.client.host, port=request.client.port) if request.client else request.client,
            protocol=PROTOCOL_CONNECT,
            query=request.query_params,
        )

        conn: StreamingHandlerConn
        if self.params.spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryHandlerConn(
                writer=writer,
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
            conn = ConnectStreamingHandlerConn(
                writer=writer,
                request=request,
                peer=peer,
                spec=self.params.spec,
                marshaler=ConnectStreamingMarshaler(
                    codec=codec,
                    compress_min_bytes=self.params.compress_min_bytes,
                    send_max_bytes=self.params.send_max_bytes,
                    compression=response_compression,
                ),
                unmarshaler=ConnectStreamingUnmarshaler(
                    stream=request.stream(),
                    codec=codec,
                    compression=request_compression,
                    read_max_bytes=self.params.read_max_bytes,
                ),
                request_headers=Headers(request.headers, encoding="latin-1"),
                response_headers=response_headers,
                response_trailers=response_trailers,
            )

        if error:
            await conn.send_error(error)
            return None

        return conn


class ConnectUnaryHandlerConn(StreamingHandlerConn):
    """ConnectUnaryHandlerConn is a handler connection class for unary RPCs in the Connect protocol.

    Attributes:
        request (Request): The incoming request object.
        marshaler (ConnectUnaryMarshaler): An instance of ConnectUnaryMarshaler used to marshal messages.
        unmarshaler (ConnectUnaryUnmarshaler): An instance of ConnectUnaryUnmarshaler used to unmarshal messages.
        headers (Headers): The headers for the response.

    """

    writer: ServerResponseWriter
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
        writer: ServerResponseWriter,
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
            writer (ServerResponseWriter): The writer to send the response.
            request (Request): The incoming request object.
            peer (Peer): The peer information.
            spec (Spec): The specification object.
            marshaler (ConnectUnaryMarshaler): The marshaler to serialize data.
            unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler to deserialize data.
            request_headers (Headers): The headers for the request.
            response_headers (Headers): The headers for the response.
            response_trailers (Headers, optional): The trailers for the response.

        """
        self.writer = writer
        self.request = request
        self._peer = peer
        self._spec = spec
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers if response_trailers is not None else Headers()

    def parse_timeout(self) -> float | None:
        """Parse the timeout value."""
        try:
            timeout = self.request.headers.get(CONNECT_HEADER_TIMEOUT)
            if timeout is None:
                return None

            timeout_ms = int(timeout)
        except ValueError as e:
            raise ConnectError(f"parse timeout: {str(e)}", Code.INVALID_ARGUMENT) from e

        return timeout_ms / 1000

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

    async def _receive_messages(self, message: Any) -> AsyncIterator[Any]:
        """Receives and unmarshals a message into an object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            AsyncIterator[Any]: An async iterator yielding the unmarshaled object.

        """
        yield await self.unmarshaler.unmarshal(message)

    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message, unmarshals it, and returns the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            AsyncIterator[Any]: An async iterator yielding the unmarshaled object.

        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Retrieve the headers from the request.

        Returns:
            Mapping[str, str]: A dictionary-like object containing the request headers.

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
        self.merge_response_trailers()

        message = await ensure_single(messages)

        data = self.marshaler.marshal(message)
        await self.writer.write(Response(data, HTTPStatus.OK, self.response_headers))

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

    async def send_error(self, error: ConnectError) -> None:
        """Send an error response.

        This method updates the response headers with the error metadata,
        sets the response trailers, converts the error code to an HTTP status code,
        serializes the error to JSON, and writes the response.

        Args:
            error (ConnectError): The error to be sent in the response.

        Returns:
            None

        """
        if not error.wire_error:
            self.response_headers.update(exclude_protocol_headers(error.metadata))

        self.merge_response_trailers()

        status_code = connect_code_to_http(error.code)
        self.response_headers[HEADER_CONTENT_TYPE] = CONNECT_UNARY_CONTENT_TYPE_JSON

        body = error_to_json_bytes(error)

        await self.writer.write(Response(content=body, headers=self.response_headers, status_code=status_code))

    def merge_response_trailers(self) -> None:
        """Merge response trailers into the response headers.

        This method iterates through the `_response_trailers` dictionary and adds
        each trailer key-value pair to the `_response_headers` dictionary,
        prefixing the trailer keys with `CONNECT_UNARY_TRAILER_PREFIX`.

        Returns:
            None

        """
        for key, value in self._response_trailers.items():
            self._response_headers[CONNECT_UNARY_TRAILER_PREFIX + key] = value


class ConnectStreamingHandlerConn(StreamingHandlerConn):
    """ConnectStreamingHandlerConn is a class that handles streaming connections for the Connect protocol.

    Attributes:
        writer (ServerResponseWriter): The writer used to send responses.
        request (Request): The incoming request object.
        _peer (Peer): The peer associated with this connection.
        _spec (Spec): The specification object.
        marshaler (ConnectStreamingMarshaler): The marshaler used to serialize messages.
        unmarshaler (ConnectStreamingUnmarshaler): The unmarshaler used to deserialize messages.
        _request_headers (Headers): The headers from the request.
        _response_headers (Headers): The headers for the response.
        _response_trailers (Headers): The trailers for the response.

    """

    writer: ServerResponseWriter
    request: Request
    _peer: Peer
    _spec: Spec
    marshaler: ConnectStreamingMarshaler
    unmarshaler: ConnectStreamingUnmarshaler
    _request_headers: Headers
    _response_headers: Headers
    _response_trailers: Headers

    def __init__(
        self,
        writer: ServerResponseWriter,
        request: Request,
        peer: Peer,
        spec: Spec,
        marshaler: ConnectStreamingMarshaler,
        unmarshaler: ConnectStreamingUnmarshaler,
        request_headers: Headers,
        response_headers: Headers,
        response_trailers: Headers | None = None,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            writer (ServerResponseWriter): The writer for server responses.
            request (Request): The request object.
            peer (Peer): The peer information.
            spec (Spec): The specification details.
            marshaler (ConnectStreamingMarshaler): The marshaler for streaming.
            unmarshaler (ConnectStreamingUnmarshaler): The unmarshaler for streaming.
            request_headers (Headers): The headers for the request.
            response_headers (Headers): The headers for the response.
            response_trailers (Headers, optional): The trailers for the response. Defaults to None.

        """
        self.writer = writer
        self.request = request
        self._peer = peer
        self._spec = spec
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers if response_trailers is not None else Headers()

    def parse_timeout(self) -> float | None:
        """Parse the timeout value."""
        try:
            timeout = self.request.headers.get(CONNECT_HEADER_TIMEOUT)
            if timeout is None:
                return None

            timeout_ms = int(timeout)
        except ValueError as e:
            raise ConnectError(f"parse timeout: {str(e)}", Code.INVALID_ARGUMENT) from e

        return timeout_ms / 1000

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

    async def _receive_messages(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives a message and yields unmarshaled objects.

        This method unmarshals the received message and yields each
        unmarshaled object one by one as an asynchronous iterator.

        Args:
            message (Any): The message to unmarshal.

        Returns:
            AsyncIterator[Any]: An asynchronous iterator yielding unmarshaled objects.

        Yields:
            Any: Each unmarshaled object from the message.

        """
        async for obj, _ in self.unmarshaler.unmarshal(message):
            yield obj

    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and returns an asynchronous content stream.

        This method processes the incoming message through the receive_message method
        and wraps the result in an AsyncContentStream with the appropriate stream type.

        Args:
            message (Any): The message to be processed.

        Returns:
            AsyncContentStream[Any]: An asynchronous stream of content based on the
                processed message, configured with the specification's stream type.

        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Retrieve the headers from the request.

        Returns:
            Mapping[str, str]: A dictionary-like object containing the request headers.

        """
        return self._request_headers

    async def _send_messages(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Create an async iterator that marshals messages with error handling.

        Args:
            messages (AsyncIterable[Any]): Messages to marshal

        Returns:
            AsyncIterator[bytes]: Marshaled bytes with end stream message

        Yields:
            bytes: Each marshaled message followed by an end stream message

        """
        error: ConnectError | None = None
        try:
            async for message in self.marshaler.marshal(messages):
                yield message
        except Exception as e:
            error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
        finally:
            body = self.marshaler.marshal_end_stream(error, self.response_trailers)
            yield body

    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Send a stream of messages asynchronously.

        This method marshals the provided messages and sends them using the writer.
        If an error occurs during the marshaling process, it captures the error,
        converts it to a JSON object, and sends it as the final message in the stream.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be sent.

        Returns:
            None

        Raises:
            ConnectError: If an error occurs during the marshaling process.

        """
        await self.writer.write(
            StreamingResponse(
                content=self._send_messages(messages),
                headers=self.response_headers,
                status_code=200,
            )
        )

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

    async def send_error(self, error: ConnectError) -> None:
        """Send an error response in the form of a JSON object.

        Args:
            error (ConnectError): The error object to be sent.

        Returns:
            None

        """
        body = self.marshaler.marshal_end_stream(error, self.response_trailers)

        await self.writer.write(
            StreamingResponse(content=aiterate([body]), headers=self.response_headers, status_code=200)
        )


def connect_check_protocol_version(request: Request, required: bool) -> ConnectError | None:
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
                return ConnectError(
                    f'missing required parameter: set {CONNECT_UNARY_CONNECT_QUERY_PARAMETER} to "{CONNECT_UNARY_CONNECT_QUERY_VALUE}"'
                )
            elif version is not None and version != CONNECT_UNARY_CONNECT_QUERY_VALUE:
                return ConnectError(
                    f'{CONNECT_UNARY_CONNECT_QUERY_PARAMETER} must be "{CONNECT_UNARY_CONNECT_QUERY_VALUE}": get "{version}"',
                )
        case HTTPMethod.POST:
            version = request.headers.get(CONNECT_HEADER_PROTOCOL_VERSION, None)
            if required and version is None:
                return ConnectError(
                    f'missing required header: set {CONNECT_HEADER_PROTOCOL_VERSION} to "{CONNECT_PROTOCOL_VERSION}"',
                    Code.INVALID_ARGUMENT,
                )
            elif version is not None and version != CONNECT_PROTOCOL_VERSION:
                return ConnectError(
                    f'{CONNECT_HEADER_PROTOCOL_VERSION} must be "{CONNECT_PROTOCOL_VERSION}": get "{version}"',
                    Code.INVALID_ARGUMENT,
                )
        case _:
            return ConnectError(f"unsupported method: {request.method}", Code.INVALID_ARGUMENT)

    return None


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

        return json_str.encode()
    except Exception as e:
        raise ConnectError(f"failed to serialize Connect Error: {e}", Code.INTERNAL) from e
