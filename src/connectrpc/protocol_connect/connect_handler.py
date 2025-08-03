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

"""Connect protocol handler implementation for unary and streaming RPCs."""

import json
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
)
from http import HTTPMethod, HTTPStatus
from typing import Any
from urllib.parse import unquote

from connectrpc.code import Code
from connectrpc.compression import COMPRESSION_IDENTITY
from connectrpc.connect import (
    Address,
    Peer,
    Spec,
    StreamingHandlerConn,
    StreamType,
    ensure_single,
)
from connectrpc.error import ConnectError
from connectrpc.headers import Headers
from connectrpc.protocol import (
    HEADER_CONTENT_TYPE,
    PROTOCOL_CONNECT,
    ProtocolHandler,
    ProtocolHandlerParams,
    exclude_protocol_headers,
    negotiate_compression,
)
from connectrpc.protocol_connect.base64_utils import decode_urlsafe_base64_with_padding
from connectrpc.protocol_connect.constants import (
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
from connectrpc.protocol_connect.content_type import (
    connect_codec_from_content_type,
    connect_content_type_from_codec_name,
)
from connectrpc.protocol_connect.error_code import connect_code_to_http
from connectrpc.protocol_connect.error_json import error_to_json
from connectrpc.protocol_connect.marshaler import ConnectStreamingMarshaler, ConnectUnaryMarshaler
from connectrpc.protocol_connect.unmarshaler import ConnectStreamingUnmarshaler, ConnectUnaryUnmarshaler
from connectrpc.request import Request
from connectrpc.response import Response, StreamingResponse
from connectrpc.response_writer import ServerResponseWriter
from connectrpc.utils import (
    aiterate,
)


class ConnectHandler(ProtocolHandler):
    """ConnectHandler is a protocol handler for the Connect protocol.

    Attributes:
        params (ProtocolHandlerParams): The parameters for the protocol handler, including specification and compression options.
        _methods (list[HTTPMethod]): The list of HTTP methods supported by this handler.
        accept (list[str]): The list of accepted content types.
    """

    params: ProtocolHandlerParams
    _methods: list[HTTPMethod]
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HTTPMethod], accept: list[str]) -> None:
        """Initializes the handler with the given parameters, supported HTTP methods, and accepted content types.

        Args:
            params (ProtocolHandlerParams): The parameters required for the protocol handler.
            methods (list[HTTPMethod]): A list of supported HTTP methods.
            accept (list[str]): A list of accepted content types.

        Returns:
            None
        """
        self.params = params
        self._methods = methods
        self.accept = accept

    @property
    def methods(self) -> list[HTTPMethod]:
        """Returns the list of HTTP methods supported by this handler.

        Returns:
            list[HTTPMethod]: A list containing the supported HTTP methods.
        """
        return self._methods

    def content_types(self) -> list[str]:
        """Returns a list of accepted content types.

        Returns:
            list[str]: A list of MIME types that are accepted.
        """
        return self.accept

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Determines if the handler can process the given request payload based on the content type.

        Args:
            request (Request): The incoming HTTP request object.
            content_type (str): The content type of the request payload.

        Returns:
            bool: True if the handler can accept the payload with the specified content type, False otherwise.

        Notes:
            - For GET requests, the content type may be determined from a query parameter and the stream type.
            - For other HTTP methods, the provided content_type is used directly.
        """
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
        """Handles the connection for a Connect protocol request.

        Args:
            request (Request): The incoming HTTP request object.
            response_headers (Headers): Mutable headers to be sent with the response.
            response_trailers (Headers): Mutable trailers to be sent with the response.
            writer (ServerResponseWriter): Writer for sending responses to the client.

        Returns:
            StreamingHandlerConn | None: A connection handler for the request, or None if an error occurred.

        Workflow:
            - Determines stream type (Unary or Streaming) and negotiates compression and encoding.
            - Validates protocol version and required parameters.
            - Parses and decodes the request message for unary GET requests.
            - Sets appropriate response headers based on negotiated compression and encoding.
            - Constructs and returns the appropriate connection handler (unary or streaming).
            - Sends an error response and returns None if any validation or negotiation fails.
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
                decoded = decode_urlsafe_base64_with_padding(message_unquoted)
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
            address=Address(host=request.client.host, port=request.client.port) if request.client else None,
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
    """Handler for unary Connect protocol requests.

    This class manages the lifecycle of a unary RPC connection, including
    request parsing, response serialization, error handling, and header/trailer
    management. It provides methods to receive and unmarshal incoming messages,
    marshal and send responses, and handle protocol-specific metadata.

    Attributes:
        writer (ServerResponseWriter): The writer used to send responses.
        request (Request): The incoming request object.
        _peer (Peer): Information about the remote peer.
        _spec (Spec): The protocol specification object.
        marshaler (ConnectUnaryMarshaler): Marshaler for serializing response messages.
        unmarshaler (ConnectUnaryUnmarshaler): Unmarshaler for deserializing request messages.
        _request_headers (Headers): Headers from the incoming request.
        _response_headers (Headers): Headers to be sent in the response.
        _response_trailers (Headers): Trailers to be sent in the response.
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
        """Initializes a new instance of the class.

        Args:
            writer (ServerResponseWriter): The writer used to send responses to the client.
            request (Request): The incoming request object.
            peer (Peer): Information about the remote peer.
            spec (Spec): The specification for the current operation.
            marshaler (ConnectUnaryMarshaler): The marshaler for serializing responses.
            unmarshaler (ConnectUnaryUnmarshaler): The unmarshaler for deserializing requests.
            request_headers (Headers): Headers from the incoming request.
            response_headers (Headers): Headers to include in the response.
            response_trailers (Headers | None, optional): Trailers to include in the response. Defaults to None.

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
        """Parses the timeout value from the request headers.

        Retrieves the timeout value from the `CONNECT_HEADER_TIMEOUT` header in the request.
        If the header is not present, returns None. If present, attempts to convert the value
        to an integer (milliseconds), and returns the timeout in seconds as a float.

        Raises:
            ConnectError: If the timeout value cannot be converted to an integer.

        Returns:
            float | None: The timeout value in seconds, or None if not specified.
        """
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
        """Returns the specification object associated with this handler.

        Returns:
            Spec: The specification instance for this handler.
        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this handler.

        Returns:
            Peer: The Peer object containing information about the remote peer.
        """
        return self._peer

    async def _receive_messages(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives and unmarshals a message, yielding the result.

        Args:
            message (Any): The raw message to be unmarshaled.

        Yields:
            Any: The unmarshaled message object.

        """
        yield await self.unmarshaler.unmarshal(message)

    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and returns an asynchronous iterator over the processed messages.

        Args:
            message (Any): The input message to be processed.

        Returns:
            AsyncIterator[Any]: An asynchronous iterator yielding processed messages.
        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Returns the HTTP headers associated with the current request.

        Returns:
            Headers: The headers of the request.
        """
        return self._request_headers

    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Sends a single message over the connection.

        This asynchronous method expects an asynchronous iterable of messages,
        ensures that only a single message is present, marshals it, and writes
        the response using the provided writer. Response trailers are merged
        before sending the message.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable containing the message to send.

        Raises:
            ValueError: If the iterable contains zero or more than one message.
            Exception: Propagates exceptions raised during marshaling or writing.
        """
        self.merge_response_trailers()

        message = await ensure_single(messages)

        data = self.marshaler.marshal(message)
        await self.writer.write(Response(data, HTTPStatus.OK, self.response_headers))

    @property
    def response_headers(self) -> Headers:
        """Returns the HTTP response headers.

        Returns:
            Headers: The headers of the HTTP response.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the HTTP response trailers as a Headers object.

        Response trailers are additional HTTP headers sent after the response body,
        typically used in protocols like gRPC or HTTP/2 for metadata that is only
        available once the response body has been generated.

        Returns:
            Headers: The response trailers associated with the HTTP response.
        """
        return self._response_trailers

    def get_http_method(self) -> HTTPMethod:
        """Returns the HTTP method of the current request as an `HTTPMethod` enum.

        Returns:
            HTTPMethod: The HTTP method (e.g., GET, POST) of the request.
        """
        return HTTPMethod(self.request.method)

    async def send_error(self, error: ConnectError) -> None:
        """Sends an error response to the client in the Connect protocol format.

        Args:
            error (ConnectError): The error object containing error details, code, and metadata.

        Behavior:
            - Updates response headers with error metadata, excluding protocol-specific headers if `wire_error` is False.
            - Merges response trailers into the headers.
            - Sets the appropriate HTTP status code based on the Connect error code.
            - Sets the response content type to Connect JSON.
            - Serializes the error to JSON bytes and writes the response to the client.
        """
        if not error.wire_error:
            self.response_headers.update(exclude_protocol_headers(error.metadata))

        self.merge_response_trailers()

        status_code = connect_code_to_http(error.code)
        self.response_headers[HEADER_CONTENT_TYPE] = CONNECT_UNARY_CONTENT_TYPE_JSON

        body = error_to_json_bytes(error)

        await self.writer.write(Response(content=body, headers=self.response_headers, status_code=status_code))

    def merge_response_trailers(self) -> None:
        """Merges the response trailers into the response headers by prefixing each trailer key with CONNECT_UNARY_TRAILER_PREFIX and adding it to the response headers dictionary.

        This is typically used to ensure that trailer metadata is included in the headers
        for protocols or transports that do not natively support trailers.

        Returns:
            None
        """
        for key, value in self._response_trailers.items():
            self._response_headers[CONNECT_UNARY_TRAILER_PREFIX + key] = value


class ConnectStreamingHandlerConn(StreamingHandlerConn):
    """ConnectStreamingHandlerConn manages the lifecycle and data flow of a streaming connection using the Connect protocol.

    It handles marshaling and unmarshaling of streaming messages, manages request and response
    headers/trailers, and provides methods for sending and receiving messages asynchronously.
    This class is designed to work with a server response writer and encapsulates protocol-specific
    logic for error handling and timeout parsing.

    Attributes:
        writer (ServerResponseWriter): The writer used to send responses to the client.
        request (Request): The incoming request object.
        _peer (Peer): Information about the remote peer.
        _spec (Spec): The protocol specification details.
        marshaler (ConnectStreamingMarshaler): Marshals outgoing streaming messages.
        unmarshaler (ConnectStreamingUnmarshaler): Unmarshals incoming streaming messages.
        _request_headers (Headers): Headers from the incoming request.
        _response_headers (Headers): Headers to be sent in the response.
        _response_trailers (Headers): Trailers to be sent at the end of the response stream.
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
        """Initializes the ConnectHandler with the provided writer, request, peer, specification, marshaler, unmarshaler, and headers.

        Args:
            writer (ServerResponseWriter): The writer used to send responses to the client.
            request (Request): The incoming request object.
            peer (Peer): The peer information for the connection.
            spec (Spec): The specification for the connection.
            marshaler (ConnectStreamingMarshaler): The marshaler for streaming responses.
            unmarshaler (ConnectStreamingUnmarshaler): The unmarshaler for streaming requests.
            request_headers (Headers): Headers from the incoming request.
            response_headers (Headers): Headers to include in the response.
            response_trailers (Headers | None, optional): Trailing headers to include in the response. Defaults to None.

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
        """Parses the timeout value from the request headers.

        Retrieves the timeout value specified in the CONNECT_HEADER_TIMEOUT header,
        converts it from milliseconds to seconds, and returns it as a float.
        If the header is not present, returns None.
        Raises a ConnectError with Code.INVALID_ARGUMENT if the header value is not a valid integer.

        Returns:
            float | None: The timeout value in seconds, or None if not specified.

        Raises:
            ConnectError: If the timeout value cannot be converted to an integer.
        """
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
        """Returns the specification object associated with this handler.

        Returns:
            Spec: The specification instance for this handler.
        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object for this handler.

        Returns:
            Peer: The Peer object containing information about the remote peer.
        """
        return self._peer

    async def _receive_messages(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives and yields unmarshaled message objects.

        Args:
            message (Any): The incoming message to be unmarshaled.

        Yields:
            Any: Each unmarshaled object extracted from the message.

        Raises:
            Any exceptions raised by the unmarshaler during processing.
        """
        async for obj, _ in self.unmarshaler.unmarshal(message):
            yield obj

    def receive(self, message: Any) -> AsyncIterator[Any]:
        """Receives a message and returns an asynchronous iterator over the processed messages.

        Args:
            message (Any): The message to be received and processed.

        Returns:
            AsyncIterator[Any]: An asynchronous iterator yielding processed messages.
        """
        return self._receive_messages(message)

    @property
    def request_headers(self) -> Headers:
        """Returns the HTTP headers associated with the current request.

        Returns:
            Headers: The headers of the request.
        """
        return self._request_headers

    async def _send_messages(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Asynchronously sends marshaled messages and yields them as byte streams.

        Iterates over the provided asynchronous iterable of messages, marshals each message,
        and yields the resulting bytes. If an exception occurs during marshaling, it captures
        the error and ensures that an end-of-stream message is marshaled and yielded with
        appropriate error information and response trailers.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled and sent.

        Yields:
            bytes: Marshaled message bytes, including a final end-of-stream message.

        Raises:
            ConnectError: If an internal error occurs during marshaling.
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
        """Asynchronously sends a stream of messages to the client using a streaming HTTP response.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be sent to the client.

        Returns:
            None

        Raises:
            Any exceptions raised by the writer or during message streaming will propagate.
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
        """Returns the HTTP response headers.

        Returns:
            Headers: The headers included in the HTTP response.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the HTTP response trailers.

        Response trailers are additional headers sent after the response body, typically used in protocols like HTTP/2 or gRPC to provide metadata that is only available once the response body has been generated.

        Returns:
            Headers: The response trailers as a Headers object.
        """
        return self._response_trailers

    async def send_error(self, error: ConnectError) -> None:
        """Sends an error response to the client using the provided ConnectError.

        This method marshals the error and response trailers into a response body,
        then writes a streaming HTTP response with the appropriate headers and a status code of 200.

        Args:
            error (ConnectError): The error to be sent to the client.

        Returns:
            None
        """
        body = self.marshaler.marshal_end_stream(error, self.response_trailers)

        await self.writer.write(
            StreamingResponse(content=aiterate([body]), headers=self.response_headers, status_code=200)
        )


def connect_check_protocol_version(request: Request, required: bool) -> ConnectError | None:
    """Validates the protocol version in a Connect request based on the HTTP method.

    For GET requests, checks the presence and value of a specific query parameter.
    For POST requests, checks the presence and value of a specific header.
    Returns a ConnectError if the required protocol version is missing or incorrect,
    or if the HTTP method is unsupported. Returns None if the protocol version is valid.

    Args:
        request (Request): The incoming HTTP request to validate.
        required (bool): Whether the protocol version is required.

    Returns:
        ConnectError | None: A ConnectError describing the validation failure, or None if valid.
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
    """Serializes a ConnectError object to a JSON-formatted bytes object.

    Args:
        error (ConnectError): The ConnectError instance to serialize.

    Returns:
        bytes: The JSON representation of the error, encoded as UTF-8 bytes.

    Raises:
        ConnectError: If serialization fails, raises a new ConnectError with an INTERNAL code.
    """
    try:
        json_obj = error_to_json(error)
        json_str = json.dumps(json_obj)

        return json_str.encode()
    except Exception as e:
        raise ConnectError(f"failed to serialize Connect Error: {e}", Code.INTERNAL) from e
