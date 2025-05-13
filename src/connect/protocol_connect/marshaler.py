"""Provides marshaling utilities for the Connect protocol."""

import base64
import contextlib
import json
from typing import Any

from yarl import URL

from connect.code import Code
from connect.codec import Codec, StableCodec
from connect.compression import Compression
from connect.envelope import EnvelopeFlags, EnvelopeWriter
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol import (
    HEADER_CONTENT_ENCODING,
    HEADER_CONTENT_LENGTH,
    HEADER_CONTENT_TYPE,
)
from connect.protocol_connect.constants import (
    CONNECT_HEADER_PROTOCOL_VERSION,
    CONNECT_UNARY_BASE64_QUERY_PARAMETER,
    CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER,
    CONNECT_UNARY_CONNECT_QUERY_PARAMETER,
    CONNECT_UNARY_CONNECT_QUERY_VALUE,
    CONNECT_UNARY_ENCODING_QUERY_PARAMETER,
    CONNECT_UNARY_HEADER_COMPRESSION,
    CONNECT_UNARY_MESSAGE_QUERY_PARAMETER,
)
from connect.protocol_connect.end_stream import end_stream_to_json


class ConnectUnaryMarshaler:
    """ConnectUnaryMarshaler is responsible for serializing and optionally compressing messages.

    Attributes:
        codec (Codec): The codec used for serializing messages.
        compression (Compression | None): The compression method used for compressing messages, if any.
        compress_min_bytes (int): The minimum size in bytes for a message to be compressed.
        send_max_bytes (int): The maximum allowed size in bytes for a message to be sent.
        headers (Headers | Headers): The headers to be included in the message.

    """

    codec: Codec | None
    compression: Compression | None
    compress_min_bytes: int
    send_max_bytes: int
    headers: Headers

    def __init__(
        self,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
        headers: Headers,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            codec (Codec): The codec to be used for encoding/decoding.
            compression (Compression | None): The compression method to be used, or None if no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes to send in a single message.
            headers (Headers): The headers to be included in the connection.

        Returns:
            None

        """
        self.codec = codec
        self.compression = compression
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.headers = headers

    def marshal(self, message: Any) -> bytes:
        """Marshals a message into bytes, optionally compressing it if it exceeds a certain size.

        Args:
            message (Any): The message to be marshaled.

        Returns:
            bytes: The marshaled (and possibly compressed) message.

        Raises:
            ConnectError: If there is an error during marshaling or if the message size exceeds the allowed limit.

        """
        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        try:
            data = self.codec.marshal(message)
        except Exception as e:
            raise ConnectError(f"marshal message: {str(e)}", Code.INTERNAL) from e

        if len(data) < self.compress_min_bytes or self.compression is None:
            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                raise ConnectError(
                    f"message size {len(data)} exceeds send_max_bytes {self.send_max_bytes}", Code.RESOURCE_EXHAUSTED
                )

            return data

        data = self.compression.compress(data)

        if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
            raise ConnectError(
                f"compressed message size {len(data)} exceeds send_max_bytes {self.send_max_bytes}",
                Code.RESOURCE_EXHAUSTED,
            )

        self.headers[CONNECT_UNARY_HEADER_COMPRESSION] = self.compression.name

        return data


class ConnectUnaryRequestMarshaler(ConnectUnaryMarshaler):
    """ConnectUnaryRequestMarshaler is responsible for marshaling unary request messages for the Connect protocol, with support for GET requests and stable codecs.

    This class extends ConnectUnaryMarshaler to provide additional functionality for handling GET requests,
    including marshaling messages using a stable codec, enforcing message size limits, and optionally compressing
    messages when necessary. It also manages the construction of GET URLs with appropriate query parameters and
    headers for the Connect protocol.

    Attributes:
        enable_get (bool): Flag indicating whether GET requests are enabled.
        stable_codec (StableCodec | None): The codec used for stable marshaling, if available.
        url (URL | None): The URL to use for the request.

    """

    enable_get: bool
    stable_codec: StableCodec | None
    url: URL | None

    def __init__(
        self,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
        headers: Headers,
        enable_get: bool = False,
        stable_codec: StableCodec | None = None,
        url: URL | None = None,
    ) -> None:
        """Initialize the protocol connection with the specified configuration.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding messages, or None.
            compression (Compression | None): The compression algorithm to use, or None.
            compress_min_bytes (int): Minimum number of bytes before compression is applied.
            send_max_bytes (int): Maximum number of bytes allowed per send operation.
            headers (Headers): Headers to include in each request.
            enable_get (bool, optional): Whether to enable GET requests. Defaults to False.
            stable_codec (StableCodec | None, optional): An optional stable codec for message encoding/decoding. Defaults to None.
            url (URL | None, optional): The URL endpoint for the connection. Defaults to None.

        Returns:
            None

        """
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes, headers)
        self.enable_get = enable_get
        self.stable_codec = stable_codec
        self.url = url

    def marshal(self, message: Any) -> bytes:
        """Marshal a message into bytes.

        If `enable_get` is True and `stable_codec` is None, raises a `ConnectError`
        indicating that the codec does not support stable marshal and cannot use get.
        Otherwise, if `enable_get` is True and `stable_codec` is not None, marshals
        the message using the `marshal_with_get` method.

        If `enable_get` is False, marshals the message using the `.

        Args:
            message (Any): The message to be marshaled.

        Returns:
            bytes: The marshaled message in bytes.

        Raises:
            ConnectError: If `enable_get` is True and `stable_codec` is None.

        """
        if self.enable_get:
            if self.codec is None:
                raise ConnectError("codec is not set", Code.INTERNAL)

            if self.stable_codec is None:
                raise ConnectError(
                    f"codec {self.codec.name} doesn't support stable marshal; can't use get",
                    Code.INTERNAL,
                )
            else:
                return self.marshal_with_get(message)

        return super().marshal(message)

    def marshal_with_get(self, message: Any) -> bytes:
        """Marshals the given message and sends it using a GET request.

        This method first marshals the message using the stable codec. If the marshaled
        data exceeds the maximum allowed size (`send_max_bytes`) and compression is not
        enabled, it raises a `ConnectError`. If the data size is within the limit, it
        builds the GET URL and sends the data.

        If the data size exceeds the limit and compression is enabled, it compresses
        the data and checks the size again. If the compressed data still exceeds the
        limit, it raises a `ConnectError`. Otherwise, it builds the GET URL with the
        compressed data and sends it.

        Args:
            message (Any): The message to be marshaled and sent.

        Returns:
            bytes: The marshaled (and possibly compressed) data.

        Raises:
            ConnectError: If the data size exceeds the maximum allowed size and compression
                          is not enabled, or if the compressed data size still exceeds the
                          limit.

        """
        if self.stable_codec is None:
            raise ConnectError("stable_codec is not set", Code.INTERNAL)

        data = self.stable_codec.marshal_stable(message)

        is_too_big = self.send_max_bytes > 0 and len(data) > self.send_max_bytes
        if is_too_big and not self.compression:
            raise ConnectError(
                f"message size {len(data)} exceeds sendMaxBytes {self.send_max_bytes}: enabling request compression may help",
                Code.RESOURCE_EXHAUSTED,
            )

        if not is_too_big:
            url = self._build_get_url(data, False)

            self._write_with_get(url)
            return data

        if self.compression:
            data = self.compression.compress(data)

        if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
            raise ConnectError(
                f"compressed message size {len(data)} exceeds send_max_bytes {self.send_max_bytes}",
                Code.RESOURCE_EXHAUSTED,
            )

        url = self._build_get_url(data, True)
        self._write_with_get(url)

        return data

    def _build_get_url(self, data: bytes, compressed: bool) -> URL:
        if self.url is None or self.stable_codec is None:
            raise ConnectError("url or stable_codec is not set", Code.INTERNAL)

        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        url = self.url
        url = url.update_query({
            CONNECT_UNARY_CONNECT_QUERY_PARAMETER: CONNECT_UNARY_CONNECT_QUERY_VALUE,
            CONNECT_UNARY_ENCODING_QUERY_PARAMETER: self.codec.name,
        })
        if self.stable_codec.is_binary() or compressed:
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8"),
                CONNECT_UNARY_BASE64_QUERY_PARAMETER: "1",
            })
        else:
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: data.decode("utf-8"),
            })

        if compressed:
            if not self.compression:
                raise ConnectError(
                    "compression must be set for compressed message",
                    Code.INTERNAL,
                )

            url = url.update_query({CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER: self.compression.name})

        return url

    def _write_with_get(self, url: URL) -> None:
        with contextlib.suppress(Exception):
            del self.headers[CONNECT_HEADER_PROTOCOL_VERSION]
            del self.headers[HEADER_CONTENT_TYPE]
            del self.headers[HEADER_CONTENT_ENCODING]
            del self.headers[HEADER_CONTENT_LENGTH]

        self.url = url


class ConnectStreamingMarshaler(EnvelopeWriter):
    """A class responsible for marshaling messages with optional compression.

    Attributes:
        codec (Codec): The codec used for marshaling messages.
        compression (Compression | None): The compression method used for compressing messages, if any.

    """

    codec: Codec | None
    compress_min_bytes: int
    send_max_bytes: int
    compression: Compression | None

    def __init__(
        self, codec: Codec | None, compression: Compression | None, compress_min_bytes: int, send_max_bytes: int
    ) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            codec (Codec): The codec to be used for encoding and decoding.
            compression (Compression | None): The compression method to be used, or None if no compression is to be applied.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes that can be sent in a single message.

        """
        self.codec = codec
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.compression = compression

    def marshal_end_stream(self, error: ConnectError | None, response_trailers: Headers) -> bytes:
        """Serialize the end-of-stream message with optional error and response trailers into a bytes envelope.

        Args:
            error (ConnectError | None): An optional error object to include in the end-of-stream message.
            response_trailers (Headers): Headers to include as response trailers.

        Returns:
            bytes: The serialized envelope containing the end-of-stream message.

        """
        json_obj = end_stream_to_json(error, response_trailers)
        json_str = json.dumps(json_obj)

        env = self.write_envelope(json_str.encode(), EnvelopeFlags.end_stream)

        return env.encode()
