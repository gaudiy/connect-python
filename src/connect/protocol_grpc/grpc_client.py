"""gRPC client implementation for Connect-Python, supporting async streaming and HTTP/2 communication."""

import asyncio
import contextlib
import functools
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping
from http import HTTPMethod
from typing import Any

import httpcore
from yarl import URL

from connect.byte_stream import HTTPCoreResponseAsyncByteStream
from connect.code import Code
from connect.codec import Codec
from connect.compression import COMPRESSION_IDENTITY, Compression, get_compresion_from_name
from connect.connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamType,
)
from connect.error import ConnectError
from connect.headers import Headers, include_request_headers
from connect.protocol import (
    HEADER_CONTENT_TYPE,
    HEADER_USER_AGENT,
    ProtocolClient,
    ProtocolClientParams,
    code_from_http_status,
)
from connect.protocol_grpc.constants import (
    DEFAULT_GRPC_USER_AGENT,
    GRPC_HEADER_ACCEPT_COMPRESSION,
    GRPC_HEADER_COMPRESSION,
    GRPC_HEADER_TIMEOUT,
    HEADER_X_USER_AGENT,
    UNIT_TO_SECONDS,
)
from connect.protocol_grpc.content_type import grpc_content_type_from_codec_name, grpc_validate_response_content_type
from connect.protocol_grpc.error_trailer import grpc_error_from_trailer
from connect.protocol_grpc.marshaler import GRPCMarshaler
from connect.protocol_grpc.unmarshaler import GRPCUnmarshaler
from connect.session import AsyncClientSession
from connect.utils import map_httpcore_exceptions

EventHook = Callable[..., Any]


class GRPCClient(ProtocolClient):
    """GRPCClient is a protocol client implementation for gRPC communication, supporting both standard and web environments.

    Attributes:
        params (ProtocolClientParams): Configuration parameters for the protocol client, including codec, compression, session, and URL.
        _peer (Peer): The peer instance associated with this client, representing the remote endpoint.
        web (bool): Indicates whether the client is running in a web environment, affecting header and content-type handling.

    """

    params: ProtocolClientParams
    _peer: Peer
    web: bool

    def __init__(self, params: ProtocolClientParams, peer: Peer, web: bool) -> None:
        """Initialize the ProtocolClient with the given parameters.

        Args:
            params (ProtocolClientParams): The parameters for the protocol client.
            peer (Peer): The peer instance to be used.
            web (bool): Indicates whether the client is running in a web environment.

        """
        self.params = params
        self._peer = peer
        self.web = web

    @property
    def peer(self) -> Peer:
        """Returns the associated Peer object.

        Returns:
            Peer: The peer instance associated with this object.

        """
        return self._peer

    def write_request_headers(self, _: StreamType, headers: Headers) -> None:
        """Set and modifies HTTP/2 or gRPC request headers based on the stream type, connection parameters, and environment.

        Args:
            stream_type (StreamType): The type of stream for which headers are being written.
            headers (Headers): The dictionary of headers to be modified or populated.

        Behavior:
            - Ensures the 'User-Agent' header is set to the default gRPC user agent if not already present.
            - If running in a web environment, also sets the 'X-User-Agent' header.
            - Sets the 'Content-Type' header according to the codec name and environment.
            - Sets the 'Accept-Encoding' header to indicate supported compression.
            - If a specific compression is configured and is not the identity, sets the gRPC compression header.
            - If multiple compressions are supported, sets the gRPC accept compression header with the supported values.
            - For non-web environments, adds the 'Te: trailers' header required for gRPC.

        Note:
            This method mutates the provided headers dictionary in place.

        """
        if headers.get(HEADER_USER_AGENT, None) is None:
            headers[HEADER_USER_AGENT] = DEFAULT_GRPC_USER_AGENT

        if self.web and headers.get(HEADER_X_USER_AGENT, None) is None:
            headers[HEADER_X_USER_AGENT] = DEFAULT_GRPC_USER_AGENT

        headers[HEADER_CONTENT_TYPE] = grpc_content_type_from_codec_name(self.web, self.params.codec.name)

        headers["Accept-Encoding"] = COMPRESSION_IDENTITY
        if self.params.compression_name and self.params.compression_name != COMPRESSION_IDENTITY:
            headers[GRPC_HEADER_COMPRESSION] = self.params.compression_name

        if self.params.compressions:
            headers[GRPC_HEADER_ACCEPT_COMPRESSION] = ", ".join(c.name for c in self.params.compressions)

        if not self.web:
            headers["Te"] = "trailers"

    def conn(self, spec: Spec, headers: Headers) -> StreamingClientConn:
        """Create and returns a GRPCClientConn instance configured with the provided specification and headers.

        Args:
            spec (Spec): The specification object defining the protocol or service interface.
            headers (Headers): The request headers to include in the connection.

        Returns:
            StreamingClientConn: An initialized gRPC streaming client connection.

        Details:
            - Configures the connection with parameters such as session, peer, URL, codec, and compression settings.
            - Initializes GRPCMarshaler and GRPCUnmarshaler with appropriate codecs and limits.
            - Compression is determined using the provided compression name and available compressions.

        """
        return GRPCClientConn(
            web=self.web,
            session=self.params.session,
            spec=spec,
            peer=self.peer,
            url=self.params.url,
            codec=self.params.codec,
            compressions=self.params.compressions,
            marshaler=GRPCMarshaler(
                codec=self.params.codec,
                compress_min_bytes=self.params.compress_min_bytes,
                send_max_bytes=self.params.send_max_bytes,
                compression=get_compresion_from_name(self.params.compression_name, self.params.compressions),
            ),
            unmarshaler=GRPCUnmarshaler(
                web=self.web,
                codec=self.params.codec,
                read_max_bytes=self.params.read_max_bytes,
            ),
            request_headers=headers,
        )


class GRPCClientConn(StreamingClientConn):
    """GRPCClientConn is a gRPC client connection implementation supporting asynchronous streaming requests and responses over HTTP/2.

    This class manages the lifecycle of a gRPC client connection, including marshaling and unmarshaling messages, handling request and response headers/trailers, managing compression, and supporting event hooks for request/response events. It integrates with an asynchronous HTTP client session and supports cancellation via asyncio events.

    Attributes:
        session (AsyncClientSession): The asynchronous client session used for HTTP requests.
        _spec (Spec): The protocol or API specification.
        _peer (Peer): Information about the remote peer.
        url (URL): The endpoint URL for the connection.
        codec (Codec | None): Codec for encoding/decoding messages.
        compressions (list[Compression]): Supported compression algorithms.
        marshaler (GRPCMarshaler): Marshaler for serializing messages.
        unmarshaler (GRPCUnmarshaler): Unmarshaler for deserializing messages.
        _response_headers (Headers): HTTP response headers.
        _response_trailers (Headers): HTTP response trailers.
        _request_headers (Headers): HTTP request headers.
        receive_trailers (Callable[[], None] | None): Callback to receive trailers after response.

    """

    web: bool
    session: AsyncClientSession
    _spec: Spec
    _peer: Peer
    url: URL
    codec: Codec | None
    compressions: list[Compression]
    marshaler: GRPCMarshaler
    unmarshaler: GRPCUnmarshaler
    _response_headers: Headers
    _response_trailers: Headers
    _request_headers: Headers
    receive_trailers: Callable[[], None] | None

    def __init__(
        self,
        web: bool,
        session: AsyncClientSession,
        spec: Spec,
        peer: Peer,
        url: URL,
        codec: Codec | None,
        compressions: list[Compression],
        request_headers: Headers,
        marshaler: GRPCMarshaler,
        unmarshaler: GRPCUnmarshaler,
        event_hooks: None | (Mapping[str, list[EventHook]]) = None,
    ) -> None:
        """Initialize a new instance of the class.

        Args:
            web (bool): Indicates if the connection is for a web environment.
            session (AsyncClientSession): The asynchronous client session to use for requests.
            spec (Spec): The specification object describing the protocol or API.
            peer (Peer): The peer information for the connection.
            url (URL): The URL endpoint for the connection.
            codec (Codec | None): The codec to use for encoding/decoding messages, or None.
            compressions (list[Compression]): List of supported compression algorithms.
            request_headers (Headers): Headers to include in outgoing requests.
            marshaler (GRPCMarshaler): The marshaler for serializing messages.
            unmarshaler (GRPCUnmarshaler): The unmarshaler for deserializing messages.
            event_hooks (None | Mapping[str, list[EventHook]], optional): Optional mapping of event hooks for "request" and "response" events. Defaults to None.

        """
        event_hooks = {} if event_hooks is None else event_hooks

        self.web = web
        self.session = session
        self._spec = spec
        self._peer = peer
        self.url = url
        self.codec = codec
        self.compressions = compressions
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._response_headers = Headers()
        self._response_trailers = Headers()
        self._request_headers = request_headers

        self._event_hooks = {
            "request": list(event_hooks.get("request", [])),
            "response": list(event_hooks.get("response", [])),
        }

    @property
    def spec(self) -> Spec:
        """Return the specification details."""
        return self._spec

    @property
    def peer(self) -> Peer:
        """Return the peer information."""
        raise NotImplementedError()

    async def receive(self, message: Any, abort_event: asyncio.Event | None) -> AsyncIterator[Any]:
        """Receives a message and processes it."""
        trailer_received = False

        async for obj, end in self.unmarshaler.unmarshal(message):
            if abort_event and abort_event.is_set():
                raise ConnectError("receive operation aborted", Code.CANCELED)

            if end:
                if trailer_received:
                    raise ConnectError("received extra end stream trailer", Code.INVALID_ARGUMENT)

                trailer_received = True
                if self.unmarshaler.web_trailers is None:
                    raise ConnectError("trailer not received", Code.INVALID_ARGUMENT)

                continue

            if trailer_received:
                raise ConnectError("protocol error: received extra message after trailer", Code.INVALID_ARGUMENT)

            yield obj

        if callable(self.receive_trailers):
            self.receive_trailers()

        if self.unmarshaler.bytes_read == 0 and len(self.response_trailers) == 0:
            self.response_trailers.update(self._response_headers)
            del self._response_headers[HEADER_CONTENT_TYPE]

            server_error = grpc_error_from_trailer(self.response_trailers)
            if server_error:
                server_error.metadata = self.response_headers.copy()
                raise server_error

        server_error = grpc_error_from_trailer(self.response_trailers)
        if server_error:
            server_error.metadata = self.response_headers.copy()
            server_error.metadata.update(self.response_trailers)
            raise server_error

    def _receive_trailers(self, response: httpcore.Response) -> None:
        if self.web:
            trailers = self.unmarshaler.web_trailers
            if trailers is not None:
                self._response_trailers.update(trailers)

        else:
            if "trailing_headers" not in response.extensions:
                return

            trailers = response.extensions["trailing_headers"]
            self._response_trailers.update(Headers(trailers))

    @property
    def request_headers(self) -> Headers:
        """Return the request headers."""
        return self._request_headers

    async def send(
        self, messages: AsyncIterable[Any], timeout: float | None, abort_event: asyncio.Event | None
    ) -> None:
        """Send a gRPC request asynchronously using HTTP/2 via httpcore, handling streaming messages, timeouts, and abort events.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled and sent as the request body.
            timeout (float | None): Optional timeout in seconds for the request. If provided, sets the gRPC timeout header.
            abort_event (asyncio.Event | None): Optional asyncio event that, if set, will abort the request and raise a cancellation error.

        Raises:
            ConnectError: If the request is aborted before or during execution, or if an error occurs during the HTTP request.

        Side Effects:
            - Invokes registered request and response event hooks.
            - Sets up the response stream and trailers for further processing.
            - Validates the HTTP response.

        """
        extensions = {}
        if timeout:
            extensions["timeout"] = {"read": timeout}
            self._request_headers[GRPC_HEADER_TIMEOUT] = grpc_encode_timeout(timeout)

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
                include_request_headers(
                    headers=self._request_headers, url=self.url, content=content_iterator, method=HTTPMethod.POST
                ).items()
            ),
            content=content_iterator,
            extensions=extensions,
        )

        for hook in self._event_hooks["request"]:
            hook(request)

        with map_httpcore_exceptions():
            if not abort_event:
                response = await self.session.pool.handle_async_request(request)
            else:
                request_task = asyncio.create_task(self.session.pool.handle_async_request(request=request))
                abort_task = asyncio.create_task(abort_event.wait())

                done, _ = await asyncio.wait({request_task, abort_task}, return_when=asyncio.FIRST_COMPLETED)

                if abort_task in done:
                    request_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await request_task

                    raise ConnectError("request aborted", Code.CANCELED)

                abort_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await abort_task

                response = await request_task

        for hook in self._event_hooks["response"]:
            hook(response)

        assert isinstance(response.stream, AsyncIterable)
        self.unmarshaler.stream = HTTPCoreResponseAsyncByteStream(aiterator=response.stream)
        self.receive_trailers = functools.partial(self._receive_trailers, response)

        await self._validate_response(response)

    async def _validate_response(self, response: httpcore.Response) -> None:
        response_headers = Headers(response.headers)
        if response.status != 200:
            raise ConnectError(
                f"HTTP {response.status}",
                code_from_http_status(response.status),
            )

        grpc_validate_response_content_type(
            self.web,
            self.marshaler.codec.name if self.marshaler.codec else "",
            response_headers.get(HEADER_CONTENT_TYPE, ""),
        )

        compression = response_headers.get(GRPC_HEADER_COMPRESSION, None)
        if compression and compression != COMPRESSION_IDENTITY:
            self.unmarshaler.compression = get_compresion_from_name(compression, self.compressions)

        self._response_headers.update(response_headers)

    @property
    def response_headers(self) -> Headers:
        """Return the response headers."""
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Return response trailers."""
        return self._response_trailers

    def on_request_send(self, fn: EventHook) -> None:
        """Register a callback function to be invoked when a request is sent.

        Args:
            fn (EventHook): The callback function to be added to the "request" event hook.

        Returns:
            None

        """
        self._event_hooks["request"].append(fn)

    async def aclose(self) -> None:
        """Asynchronously closes the underlying unmarshaler resource.

        This method should be called to properly release any resources held by the unmarshaler,
        such as open network connections or file handles, when they are no longer needed.
        """
        await self.unmarshaler.aclose()


def grpc_encode_timeout(timeout: float) -> str:
    """Encode a timeout value (in seconds) into the gRPC timeout format string.

    The gRPC timeout format is a decimal number with a time unit suffix, where the unit can be:
        - 'H' for hours
        - 'M' for minutes
        - 'S' for seconds
        - 'm' for milliseconds
        - 'u' for microseconds
        - 'n' for nanoseconds

    If the timeout is less than or equal to zero, returns "0n".

    Args:
        timeout (float): The timeout value in seconds.

    Returns:
        str: The timeout encoded as a gRPC timeout string.

    """
    if timeout <= 0:
        return "0n"

    grpc_timeout_max_value = 10**8

    _units = dict(sorted(UNIT_TO_SECONDS.items(), key=lambda item: item[1]))
    for unit, size in _units.items():
        if timeout < size * grpc_timeout_max_value:
            value = int(timeout / size)
            return f"{value}{unit}"

    value = int(timeout / 3600.0)
    return f"{value}H"
