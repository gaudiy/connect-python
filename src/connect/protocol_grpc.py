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
from connect.connect import Address, Peer, Spec, StreamingHandlerConn, UnaryHandlerConn
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
    web: bool

    def __init__(self, web: bool) -> None:
        self.web = web

    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
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
        """Implement client functionality.

        This method currently does nothing and is intended to be implemented
        in the future with the necessary client-side logic.
        """
        raise NotImplementedError()


class GRPCHandler(ProtocolHandler):
    params: ProtocolHandlerParams
    web: bool
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, web: bool, accept: list[str]) -> None:
        self.params = params
        self.web = web
        self.accept = accept

    @property
    def methods(self) -> list[HTTPMethod]:
        return GRPC_ALLOWED_METHODS

    def content_types(self) -> list[str]:
        return self.accept

    def can_handle_payload(self, _: Request, content_type: str) -> bool:
        return content_type in self.accept

    async def conn(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> UnaryHandlerConn | None:
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

    async def stream_conn(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> StreamingHandlerConn | None:
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

        conn = GRPCStreamingHandlerConn(
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
    web: bool

    def __init__(
        self,
        web: bool,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
    ) -> None:
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes)
        self.web = web

    async def marshal(self, messages: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
        async for message in self._marshal(messages):
            yield message


class GRPCUnmarshaler(EnvelopeReader):
    def __init__(
        self,
        codec: Codec | None,
        read_max_bytes: int,
        stream: AsyncIterable[bytes] | None = None,
        compression: Compression | None = None,
    ) -> None:
        super().__init__(codec, read_max_bytes, stream, compression)

    async def unmarshal(self, message: Any) -> AsyncIterator[Any]:
        async for obj, _ in self._unmarshal(message):
            yield obj


class GRPCHandlerConn(UnaryHandlerConn):
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
        self.writer = writer
        self._spec = spec
        self._peer = peer
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers if response_trailers is not None else Headers()

    def parse_timeout(self) -> float | None:
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
        return self._spec

    @property
    def peer(self) -> Peer:
        return self._peer

    async def receive(self, message: Any) -> Any:
        first = None
        async for obj in self.unmarshaler.unmarshal(message):
            # TODO(tsubakiky): validation
            if first is None:
                first = obj
            else:
                raise ConnectError("protocol error: expected only one message, but got multiple", Code.UNIMPLEMENTED)

        if first is None:
            raise ConnectError("protocol error: expected one message, but got none", Code.UNIMPLEMENTED)

        return first

    @property
    def request_headers(self) -> Headers:
        return self._request_headers

    async def send(self, message: Any) -> None:
        async def iterator() -> AsyncIterator[bytes]:
            error: ConnectError | None = None
            try:
                async for msg in self.marshaler.marshal(aiterate([message])):
                    yield msg
            except Exception as e:
                error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
            finally:
                grpc_error_to_trailer(self.response_trailers, error)

        await self.writer.write(
            StreamingResponseWithTrailers(
                content=iterator(),
                headers=self.response_headers,
                trailers=self.response_trailers,
                status_code=200,
            )
        )

    @property
    def response_headers(self) -> Headers:
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        return self._response_trailers

    async def send_error(self, error: ConnectError) -> None:
        grpc_error_to_trailer(self.response_trailers, error)

        await self.writer.write(
            StreamingResponseWithTrailers(
                content=aiterate([b""]), headers=self.response_headers, trailers=self.response_trailers, status_code=200
            )
        )


class GRPCStreamingHandlerConn(StreamingHandlerConn):
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
        self.writer = writer
        self._spec = spec
        self._peer = peer
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler
        self._request_headers = request_headers
        self._response_headers = response_headers
        self._response_trailers = response_trailers if response_trailers is not None else Headers()

    def parse_timeout(self) -> float | None:
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
        return self._spec

    @property
    def peer(self) -> Peer:
        return self._peer

    async def receive(self, message: Any) -> AsyncIterator[Any]:
        async for obj in self.unmarshaler.unmarshal(message):
            # TODO(tsubakiky): validation
            yield obj

    @property
    def request_headers(self) -> Headers:
        return self._request_headers

    async def send(self, messages: AsyncIterable[Any]) -> None:
        async def iterator() -> AsyncIterator[bytes]:
            error: ConnectError | None = None
            try:
                async for msg in self.marshaler.marshal(messages):
                    yield msg
            except Exception as e:
                error = e if isinstance(e, ConnectError) else ConnectError("internal error", Code.INTERNAL)
            finally:
                grpc_error_to_trailer(self.response_trailers, error)

        await self.writer.write(
            StreamingResponseWithTrailers(
                content=iterator(),
                headers=self.response_headers,
                trailers=self.response_trailers,
                status_code=200,
            )
        )

    @property
    def response_headers(self) -> Headers:
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        return self._response_trailers

    async def send_error(self, error: ConnectError) -> None:
        grpc_error_to_trailer(self.response_trailers, error)

        await self.writer.write(
            StreamingResponseWithTrailers(
                content=aiterate([b""]), headers=self.response_headers, trailers=self.response_trailers, status_code=200
            )
        )


def grpc_codec_from_content_type(web: bool, content_type: str) -> str:
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
