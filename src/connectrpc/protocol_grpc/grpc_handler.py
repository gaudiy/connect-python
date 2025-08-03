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

"""gRPC and gRPC-web protocol handler and connection classes for Connect Python framework."""

from collections.abc import AsyncIterable, AsyncIterator
from http import HTTPMethod
from typing import Any

from connectrpc.code import Code
from connectrpc.compression import COMPRESSION_IDENTITY
from connectrpc.connect import (
    Address,
    Peer,
    Spec,
    StreamingHandlerConn,
)
from connectrpc.error import ConnectError
from connectrpc.headers import Headers
from connectrpc.protocol import (
    HEADER_CONTENT_TYPE,
    PROTOCOL_GRPC,
    ProtocolHandler,
    ProtocolHandlerParams,
    negotiate_compression,
)
from connectrpc.protocol_grpc.constants import (
    GRPC_ALLOWED_METHODS,
    GRPC_HEADER_ACCEPT_COMPRESSION,
    GRPC_HEADER_COMPRESSION,
    GRPC_HEADER_TIMEOUT,
    GRPC_TIMEOUT_MAX_DURATION,
    MAX_HOURS,
    RE_TIMEOUT,
    UNIT_TO_SECONDS,
)
from connectrpc.protocol_grpc.content_type import grpc_codec_from_content_type
from connectrpc.protocol_grpc.error_trailer import grpc_error_to_trailer
from connectrpc.protocol_grpc.marshaler import GRPCMarshaler
from connectrpc.protocol_grpc.unmarshaler import GRPCUnmarshaler
from connectrpc.request import Request
from connectrpc.response import StreamingResponse
from connectrpc.response_writer import ServerResponseWriter
from connectrpc.utils import aiterate


class GRPCHandler(ProtocolHandler):
    """GRPCHandler is a protocol handler for managing gRPC and gRPC-web requests.

    This class is responsible for handling incoming gRPC protocol requests, negotiating compression,
    selecting codecs, and managing the connection lifecycle for both standard gRPC and gRPC-web protocols.
    It provides methods to determine accepted HTTP methods, supported content types, and whether a given
    payload can be handled. The main entry point for handling a connection is the asynchronous `conn` method,
    which negotiates protocol details and returns a connection handler for streaming communication.

    Attributes:
        params (ProtocolHandlerParams): Configuration parameters for the protocol handler, including codecs and compressions.
        web (bool): Indicates if the handler is for gRPC-web protocol.
        accept (list[str]): List of accepted MIME content types.
    """

    params: ProtocolHandlerParams
    web: bool
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, web: bool, accept: list[str]) -> None:
        """Initializes the handler with the given parameters.

        Args:
            params (ProtocolHandlerParams): The parameters for the protocol handler.
            web (bool): Indicates whether the handler is for web usage.
            accept (list[str]): List of accepted content types.
        """
        self.params = params
        self.web = web
        self.accept = accept

    @property
    def methods(self) -> list[HTTPMethod]:
        """Returns a list of allowed HTTP methods for gRPC handlers.

        Returns:
            list[HTTPMethod]: The list of HTTP methods permitted for gRPC endpoints.
        """
        return GRPC_ALLOWED_METHODS

    def content_types(self) -> list[str]:
        """Returns a list of accepted content types.

        Returns:
            list[str]: A list of MIME types that are accepted.
        """
        return self.accept

    def can_handle_payload(self, _: Request, content_type: str) -> bool:
        """Determines if the handler can process a request with the specified content type.

        Args:
            _ (Request): The incoming request object (unused).
            content_type (str): The MIME type of the request payload.

        Returns:
            bool: True if the content type is accepted by the handler, False otherwise.
        """
        return content_type in self.accept

    async def conn(
        self,
        request: Request,
        response_headers: Headers,
        response_trailers: Headers,
        writer: ServerResponseWriter,
    ) -> StreamingHandlerConn | None:
        """Handles the setup of a gRPC streaming connection, negotiating compression, codecs, and protocol details.

        Args:
            request (Request): The incoming gRPC request object containing headers and client information.
            response_headers (Headers): Headers to be sent in the response.
            response_trailers (Headers): Trailers to be sent at the end of the response.
            writer (ServerResponseWriter): The writer used to send responses to the client.

        Returns:
            StreamingHandlerConn | None: Returns a configured GRPCHandlerConn instance for handling the connection,
            or None if an error occurred during negotiation (in which case an error is sent to the client).

        Side Effects:
            - Negotiates compression and codec based on request and server capabilities.
            - Sets appropriate response headers for gRPC protocol.
            - Sends an error to the client and returns None if negotiation fails.
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
            address=Address(host=request.client.host, port=request.client.port) if request.client else None,
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
    """GRPCHandlerConn is a connection handler for gRPC protocol requests, supporting both standard and web environments.

    This class manages the lifecycle of a gRPC connection, including parsing request headers, handling message
    marshaling/unmarshaling, managing response headers and trailers, and sending responses or errors to the client.
    It supports both streaming and unary operations, and can adapt its behavior for web-based gRPC requests.

    Attributes:
        web (bool): Indicates if the connection is for a web environment.
        writer (ServerResponseWriter): The writer used to send responses to the client.
        marshaler (GRPCMarshaler): Marshals response messages into bytes.
        unmarshaler (GRPCUnmarshaler): Unmarshals request messages from bytes.
        _spec (Spec): The protocol or service specification.
        _peer (Peer): Information about the remote peer.
        _request_headers (Headers): Headers received with the request.
        _response_headers (Headers): Headers to include in the response.
        _response_trailers (Headers): Trailers to include in the response.
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
        """Initializes a new instance of the class.

        Args:
            web (bool): Indicates if the handler is for a web context.
            writer (ServerResponseWriter): The response writer for sending server responses.
            spec (Spec): The specification object for the gRPC protocol.
            peer (Peer): The peer information for the connection.
            marshaler (GRPCMarshaler): The marshaler for serializing responses.
            unmarshaler (GRPCUnmarshaler): The unmarshaler for deserializing requests.
            request_headers (Headers): The headers received in the request.
            response_headers (Headers): The headers to be sent in the response.
            response_trailers (Headers | None, optional): The trailers to be sent in the response. Defaults to None.

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
        """Parses the gRPC timeout value from the request headers and returns it as seconds.

        Returns:
            float | None: The timeout value in seconds if present and valid, otherwise None.

        Raises:
            ConnectError: If the timeout value is present but invalid or exceeds the maximum allowed duration.

        Notes:
            - If the timeout unit is hours and exceeds the maximum allowed hours, None is returned.
            - The timeout is extracted from the request headers using the GRPC_HEADER_TIMEOUT key.
        """
        timeout = self._request_headers.get(GRPC_HEADER_TIMEOUT)
        if not timeout:
            return None

        m = RE_TIMEOUT.match(timeout)
        if m is None:
            raise ConnectError(f"protocol error: invalid grpc timeout value: {timeout}")

        num_str, unit = m.groups()
        num = int(num_str)
        if num > GRPC_TIMEOUT_MAX_DURATION:
            raise ConnectError(f"protocol error: timeout {timeout!r} is too long")

        if unit == "H" and num > MAX_HOURS:
            return None

        seconds = num * UNIT_TO_SECONDS[unit]
        return seconds

    @property
    def spec(self) -> Spec:
        """Returns the specification object associated with this handler.

        Returns:
            Spec: The specification instance for this handler.
        """
        return self._spec

    @property
    def peer(self) -> Peer:
        """Returns the current Peer instance associated with this handler.

        :returns: The Peer object representing the current peer.
        :rtype: Peer
        """
        return self._peer

    async def receive(self, message: Any) -> AsyncIterator[Any]:
        """Asynchronously receives and yields deserialized objects from the given message.

        Args:
            message (Any): The incoming message to be unmarshaled.

        Yields:
            Any: Each deserialized object extracted from the message.
        """
        async for obj, _ in self.unmarshaler.unmarshal(message):
            yield obj

    @property
    def request_headers(self) -> Headers:
        """Returns the headers associated with the current request.

        Returns:
            Headers: The headers of the current request.
        """
        return self._request_headers

    async def send(self, messages: AsyncIterable[Any]) -> None:
        """Asynchronously sends messages to the client using a streaming response.

        Depending on the `web` attribute, constructs and writes a `StreamingResponse` with appropriate headers and optional trailers.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be sent to the client.

        Returns:
            None

        Raises:
            Any exceptions raised by the underlying writer or StreamingResponse.
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
        """Returns the response headers associated with the current gRPC call.

        Returns:
            Headers: The headers sent in the gRPC response.
        """
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers as a Headers object.

        Response trailers are additional HTTP headers sent after the response body in gRPC communication.
        They may contain metadata or status information relevant to the response.

        Returns:
            Headers: The response trailers associated with the gRPC response.
        """
        return self._response_trailers

    async def _send_messages(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Asynchronously sends marshaled messages and yields them as bytes.

        Iterates over the provided asynchronous iterable of messages, marshals each message,
        and yields the resulting bytes. Handles exceptions by converting them to a ConnectError
        if necessary, and appends error information to the response trailers. If running in a web
        context, marshals and yields the response trailers as the final message.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled and sent.

        Yields:
            bytes: The marshaled message bytes, and optionally marshaled web trailers if in a web context.

        Raises:
            ConnectError: If an internal error occurs during marshaling or sending.
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
        """Sends an error response using gRPC error trailers.

        Depending on the context (web or non-web), this method serializes and writes the error information
        to the response stream. For web clients, it marshals the trailers and writes them as the response body.
        For non-web clients, it sends the trailers directly.

        Args:
            error (ConnectError): The error to be sent in the response.

        Returns:
            None
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
