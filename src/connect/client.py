"""Provide the Client and ClientConfig classes for making unary calls.

These classes allow making unary calls to a specified URL with given request and response types.
"""

import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import httpcore
from yarl import URL

from connect.code import Code
from connect.codec import Codec, ProtoBinaryCodec
from connect.compression import COMPRESSION_IDENTITY, Compression, GZipCompression, get_compresion_from_name
from connect.connect import (
    Spec,
    StreamRequest,
    StreamResponse,
    StreamType,
    UnaryRequest,
    UnaryResponse,
    recieve_stream_response,
    recieve_unary_response,
)
from connect.error import ConnectError
from connect.idempotency_level import IdempotencyLevel
from connect.interceptor import apply_interceptors
from connect.options import ClientOptions
from connect.protocol import ProtocolClient, ProtocolClientParams
from connect.protocol_connect import ProtocolConnect
from connect.session import AsyncClientSession


def parse_request_url(raw_url: str) -> URL:
    """Parse the given raw URL string and returns a URL object.

    Args:
        raw_url (str): The raw URL string to be parsed.

    Returns:
        URL: The parsed URL object.

    Raises:
        ConnectError: If the URL does not have a valid scheme (http or https).

    """
    url = URL(raw_url)

    if url.scheme not in ["http", "https"]:
        raise ConnectError(
            f"URL {raw_url} missing scheme: use http:// or https://",
            Code.UNAVAILABLE,
        )

    return url


class ClientConfig:
    """Configuration class for a client.

    Attributes:
        url (URL): The URL of the client.
        protocol (ProtocolConnect): The protocol used for connection.
        procedure (str): The procedure path derived from the URL.
        codec (Codec): The codec used for encoding/decoding.
        request_compression_name (str | None): The name of the request compression method.
        compressions (list[Compression]): List of compression methods.
        descriptor (Any): The descriptor for the client.
        idempotency_level (IdempotencyLevel): The idempotency level of the client.
        compress_min_bytes (int): Minimum bytes for compression.
        read_max_bytes (int): Maximum bytes to read.
        send_max_bytes (int): Maximum bytes to send.

    """

    url: URL
    protocol: ProtocolConnect
    procedure: str
    codec: Codec
    request_compression_name: str | None
    compressions: list[Compression]
    descriptor: Any
    idempotency_level: IdempotencyLevel
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    enable_get: bool

    def __init__(self, raw_url: str, options: ClientOptions):
        """Initialize the client with the given URL and options.

        Args:
            raw_url (str): The raw URL to connect to.
            options (ClientOptions): The options for the client configuration.

        Attributes:
            url (ParseResult): The parsed URL.
            protocol (ProtocolConnect): The protocol used for connection.
            procedure (str): The procedure path extracted from the URL.
            codec (ProtoBinaryCodec): The codec used for encoding/decoding messages.
            request_compression_name (str): The name of the request compression method.
            compressions (list): The list of compression methods.
            descriptor (Descriptor): The descriptor for the client.
            idempotency_level (int): The idempotency level for requests.
            compress_min_bytes (int): The minimum number of bytes to trigger compression.
            read_max_bytes (int): The maximum number of bytes to read.
            send_max_bytes (int): The maximum number of bytes to send.

        """
        url = parse_request_url(raw_url)
        proto_path = url.path

        self.url = url
        self.protocol = ProtocolConnect()
        self.procedure = proto_path
        self.codec = ProtoBinaryCodec()
        self.request_compression_name = options.request_compression_name
        self.compressions = [GZipCompression()]
        if self.request_compression_name and self.request_compression_name != COMPRESSION_IDENTITY:
            compression = get_compresion_from_name(self.request_compression_name, self.compressions)
            if not compression:
                raise ConnectError(
                    f"unknown compression: {self.request_compression_name}",
                    Code.UNKNOWN,
                )
        self.descriptor = options.descriptor
        self.idempotency_level = options.idempotency_level
        self.compress_min_bytes = options.compress_min_bytes
        self.read_max_bytes = options.read_max_bytes
        self.send_max_bytes = options.send_max_bytes
        self.enable_get = options.enable_get

    def spec(self, stream_type: StreamType) -> Spec:
        """Generate a Spec object with the given stream type.

        Args:
            stream_type (StreamType): The type of stream to be used in the Spec.

        Returns:
            Spec: A Spec object initialized with the procedure, descriptor,
                  stream type, and idempotency level of the client.

        """
        return Spec(
            procedure=self.procedure,
            descriptor=self.descriptor,
            stream_type=stream_type,
            idempotency_level=self.idempotency_level,
        )


class Client[T_Request, T_Response]:
    """A client for making unary calls to a specified URL with given request and response types.

    Attributes:
        config (ClientConfig): Configuration for the client.
        protocol_client (ProtocolClient): The protocol client used for communication.
        _call_unary (Callable[[UnaryRequest[T_Request]], Awaitable[UnaryResponse[T_Response]]]):
            Internal method to handle unary calls.

    Methods:
        __init__(url: str, input: type[T_Request], output: type[T_Response], options: ClientOptions | None = None):
            Initialize the client with the given parameters.

        call_unary(request: UnaryRequest[T_Request]) -> UnaryResponse[T_Response]:
            Make a unary call with the given request.

    """

    config: ClientConfig
    protocol_client: ProtocolClient
    _call_unary: Callable[[UnaryRequest[T_Request]], Awaitable[UnaryResponse[T_Response]]]
    _call_stream: Callable[[StreamType, StreamRequest[T_Request]], Awaitable[StreamResponse[T_Response]]]

    def __init__(
        self,
        session: AsyncClientSession,
        url: str,
        input: type[T_Request],
        output: type[T_Response],
        options: ClientOptions | None = None,
    ):
        """Initialize the client with the given URL, request and response types, and optional client options.

        Args:
            session (AsyncClientSession): The client session to use for the connection.
            url (str): The URL of the server to connect to.
            input (type[T_Request]): The type of the request object.
            output (type[T_Response]): The type of the response object.
            options (ClientOptions | None, optional): Optional client configuration options. Defaults to None.

        Raises:
            TypeError: If the request method is not ASCII encoded.
            ConnectError: If the request or response type is incorrect.

        """
        options = options or ClientOptions()
        config = ClientConfig(url, options)
        self.config = config

        protocol_client = config.protocol.client(
            ProtocolClientParams(
                session=session,
                codec=config.codec,
                url=config.url,
                compression_name=config.request_compression_name,
                compressions=config.compressions,
                compress_min_bytes=config.compress_min_bytes,
                read_max_bytes=config.read_max_bytes,
                send_max_bytes=config.send_max_bytes,
                enable_get=config.enable_get,
            )
        )
        self.protocol_client = protocol_client

        unary_spec = config.spec(StreamType.Unary)

        async def _unary_func(request: UnaryRequest[T_Request]) -> UnaryResponse[T_Response]:
            conn = protocol_client.conn(unary_spec, request.headers)

            def on_request_send(r: httpcore.Request) -> None:
                method = r.method
                try:
                    request.method = method.decode("ascii")
                except UnicodeDecodeError as e:
                    raise TypeError(f"method must be ascii encoded: {method!r}") from e

            conn.on_request_send(on_request_send)

            await conn.send(request.message, request.timeout, abort_event=request.abort_event)

            response = await recieve_unary_response(conn=conn, t=output)
            return response

        unary_func = apply_interceptors(_unary_func, options.interceptors)

        async def call_unary(request: UnaryRequest[T_Request]) -> UnaryResponse[T_Response]:
            request.spec = unary_spec
            request.peer = protocol_client.peer
            protocol_client.write_request_headers(StreamType.Unary, request.headers)

            if not isinstance(request.message, input):
                raise ConnectError(
                    f"expected request of type: {input.__name__}",
                    Code.INTERNAL,
                )

            response = await unary_func(request)

            if not isinstance(response.message, output):
                raise ConnectError(
                    f"expected response of type: {output.__name__}",
                    Code.INTERNAL,
                )

            return response

        async def _stream_func(request: StreamRequest[T_Request]) -> StreamResponse[T_Response]:
            conn = protocol_client.stream_conn(request.spec, request.headers)

            def on_request_send(r: httpcore.Request) -> None:
                method = r.method
                try:
                    request.method = method.decode("ascii")
                except UnicodeDecodeError as e:
                    raise TypeError(f"method must be ascii encoded: {method!r}") from e

            conn.on_request_send(on_request_send)

            await conn.send(request.messages, request.timeout, request.abort_event)

            response = await recieve_stream_response(conn, output, request.spec, request.abort_event)
            return response

        stream_func = apply_interceptors(_stream_func, options.interceptors)

        async def call_stream(
            stream_type: StreamType,
            request: StreamRequest[T_Request],
        ) -> StreamResponse[T_Response]:
            request.spec = config.spec(stream_type)
            request.peer = protocol_client.peer
            protocol_client.write_request_headers(stream_type, request.headers)

            return await stream_func(request)

        self._call_unary = call_unary
        self._call_stream = call_stream

    async def call_unary(self, request: UnaryRequest[T_Request]) -> UnaryResponse[T_Response]:
        """Asynchronously calls a unary RPC (Remote Procedure Call) with the given request.

        Args:
            request (UnaryRequest[T_Request]): The request object containing the data to be sent to the server.

        Returns:
            UnaryResponse[T_Response]: The response object containing the data received from the server.

        """
        return await self._call_unary(request)

    @contextlib.asynccontextmanager
    async def call_server_stream(self, request: StreamRequest[T_Request]) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Asynchronously calls a server streaming RPC (Remote Procedure Call) with the given request.

        Args:
            request (UnaryRequest[T_Request]): The request object containing the data to be sent to the server.

        Returns:
            UnaryResponse[T_Response]: The response object containing the data received from the server.

        """
        response = await self._call_stream(StreamType.ServerStream, request)
        try:
            yield response
        finally:
            await response.aclose()

    @contextlib.asynccontextmanager
    async def call_client_stream(self, request: StreamRequest[T_Request]) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Asynchronously calls a client stream and yields responses.

        This method sends a stream request to the client and asynchronously
        iterates over the responses, yielding each response one by one.

        Args:
            request (StreamRequest[T_Request]): The stream request to be sent.

        Yields:
            UnaryResponse[T_Response]: The response from the client stream.

        """
        response = await self._call_stream(StreamType.ClientStream, request)
        try:
            yield response
        finally:
            await response.aclose()

    @contextlib.asynccontextmanager
    async def call_bidi_stream(self, request: StreamRequest[T_Request]) -> AsyncGenerator[StreamResponse[T_Response]]:
        """Initiate a bidirectional streaming call.

        This method establishes a bidirectional stream between the client and the server,
        allowing both to send and receive messages asynchronously.

        Args:
            request (StreamRequest[T_Request]): The request object containing the stream
                of messages to be sent to the server.

        Returns:
            StreamResponse[T_Response]: An asynchronous stream response object that
                allows receiving messages from the server.

        Raises:
            Any exceptions raised during the streaming call will propagate to the caller.

        """
        response = await self._call_stream(StreamType.BiDiStream, request)
        try:
            yield response
        finally:
            await response.aclose()
