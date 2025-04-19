from collections.abc import AsyncIterable, AsyncIterator
from http import HTTPMethod
from typing import Any

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
    ProtocolHandler,
    ProtocolHandlerParams,
    negotiate_compression,
)
from connect.request import Request
from connect.writer import ServerResponseWriter

GRPC_HEADER_COMPRESSION = "Grpc-Encoding"
GRPC_HEADER_ACCEPT_COMPRESSION = "Grpc-Accept-Encoding"

GRPC_CONTENT_TYPE_DEFAULT = "application/grpc"
GRPC_WEB_CONTENT_TYPE_DEFAULT = "application/grpc-web"
GRPC_CONTENT_TYPE_PREFIX = GRPC_CONTENT_TYPE_DEFAULT + "+"
GRPC_WEB_CONTENT_TYPE_PREFIX = GRPC_WEB_CONTENT_TYPE_DEFAULT + "+"

GRPC_ALLOWED_METHODS = [HTTPMethod.POST]


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
            content_types.append(f"{prefix}+{name}")

        if params.codecs.get(CodecNameType.PROTO):
            content_types.append(bare)

        return GRPCHandler(params, self.web, content_types)


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
        )

        return conn

    async def stream_conn(
        self, request: Request, response_headers: Headers, response_trailers: Headers, writer: ServerResponseWriter
    ) -> StreamingHandlerConn | None:
        raise NotImplementedError()


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
    marshaler: GRPCMarshaler
    unmarshaler: GRPCUnmarshaler

    def __init__(self, spec: Spec, peer: Peer, marshaler: GRPCMarshaler, unmarshaler: GRPCUnmarshaler) -> None:
        self._spec = spec
        self._peer = peer
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler

    @property
    def spec(self) -> Spec:
        return self._spec

    @property
    def peer(self) -> Peer:
        return self._peer

    async def receive(self, message: Any) -> Any:
        first = None
        async for obj, _ in self.unmarshaler.unmarshal(message):
            # TODO(tsubakiky): validation
            if first is None:
                first = obj
            else:
                raise ConnectError("GRPC only supports unary requests")

        return first

    @property
    def request_headers(self) -> Headers:
        raise NotImplementedError()

    async def send(self, messages: AsyncIterable[Any]) -> None:
        raise NotImplementedError()

    @property
    def response_headers(self) -> Headers:
        raise NotImplementedError()

    @property
    def response_trailers(self) -> Headers:
        raise NotImplementedError()

    async def send_error(self, error: ConnectError) -> None:
        raise NotImplementedError()


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
