"""Marshaling utilities for Connect protocol unary and streaming messages."""

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
    """ConnectUnaryMarshaler is responsible for marshaling unary messages in the Connect protocol.

    This class handles the encoding and optional compression of messages before they are sent over the network.

    Attributes:
        codec (Codec | None): Codec used for encoding/decoding messages.
        compression (Compression | None): Compression algorithm to use, or None for no compression.
        compress_min_bytes (int): Minimum message size (in bytes) before compression is applied.
        send_max_bytes (int): Maximum allowed size (in bytes) for a message to be sent.
        headers (Headers): Headers to include in the connection.
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
        """Initializes the object with the specified codec, compression settings, and headers.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding, or None if not specified.
            compression (Compression | None): The compression algorithm to use, or None for no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes allowed to send in a single message.
            headers (Headers): The headers to include with each message.
        """
        self.codec = codec
        self.compression = compression
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.headers = headers

    def marshal(self, message: Any) -> bytes:
        """Serializes and optionally compresses a message object into bytes.

        Args:
            message (Any): The message object to be marshaled.

        Returns:
            bytes: The serialized (and possibly compressed) message.

        Raises:
            ConnectError: If the codec is not set, if marshaling fails, or if the (compressed or uncompressed)
                message size exceeds the configured send_max_bytes limit.

        Process:
            - Uses the configured codec to serialize the message.
            - If the serialized data is smaller than `compress_min_bytes` or compression is not set,
              returns the data as-is (after checking size limits).
            - Otherwise, compresses the data, checks the size limit again, and sets the appropriate
              compression header before returning the compressed data.
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
    """ConnectUnaryRequestMarshaler is a specialized marshaler for unary requests in the Connect protocol.

    This class extends ConnectUnaryMarshaler and adds the ability to marshal messages for GET requests,
    optionally using a stable codec for deterministic serialization. It manages request headers, compression,
    and enforces message size limits. If GET requests are enabled, it ensures that a stable codec is available
    and handles URL construction for GET requests, including optional compression and base64 encoding.

    Attributes:
        enable_get (bool): Whether to enable GET requests for marshaling.
        stable_codec (StableCodec | None): Optional stable codec for deterministic message serialization.
        url (URL | None): The URL endpoint for the connection.
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
        """Initializes the object with the specified codec, compression, compression threshold, maximum send bytes, headers, and optional parameters.

        Args:
            codec (Codec | None): The codec to use for serialization, or None.
            compression (Compression | None): The compression method to use, or None.
            compress_min_bytes (int): Minimum number of bytes before compression is applied.
            send_max_bytes (int): Maximum number of bytes allowed to send.
            headers (Headers): Headers to include in the protocol.
            enable_get (bool, optional): Whether to enable GET requests. Defaults to False.
            stable_codec (StableCodec | None, optional): An optional stable codec for serialization. Defaults to None.
            url (URL | None, optional): An optional URL associated with the protocol. Defaults to None.
        """
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes, headers)
        self.enable_get = enable_get
        self.stable_codec = stable_codec
        self.url = url

    def marshal(self, message: Any) -> bytes:
        """Serializes the given message into bytes using the configured codec.

        If `enable_get` is True, attempts to use a stable codec for marshaling.
        Raises a ConnectError if the codec is not set or if the codec does not support stable marshaling.
        Otherwise, delegates marshaling to the superclass implementation.

        Args:
            message (Any): The message object to be serialized.

        Returns:
            bytes: The serialized message.

        Raises:
            ConnectError: If the codec is not set or does not support stable marshaling when required.
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
        """Marshals a message and sends it using a GET request, applying compression if necessary.

        Args:
            message (Any): The message object to be marshaled and sent.

        Returns:
            bytes: The marshaled (and possibly compressed) message data.

        Raises:
            ConnectError: If the stable codec is not set.
            ConnectError: If the marshaled message size exceeds `send_max_bytes` and compression is not enabled.
            ConnectError: If the compressed message size still exceeds `send_max_bytes`.

        Notes:
            - If the marshaled message size exceeds `send_max_bytes` and compression is enabled, the message will be compressed before sending.
            - The method builds the appropriate GET URL based on whether compression was applied.
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
            encoded_data = base64.urlsafe_b64encode(data).decode().rstrip("=")
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: encoded_data,
                CONNECT_UNARY_BASE64_QUERY_PARAMETER: "1",
            })
        else:
            url = url.update_query({
                CONNECT_UNARY_MESSAGE_QUERY_PARAMETER: data.decode(),
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
    """ConnectStreamingMarshaler is responsible for marshaling streaming messages in the Connect protocol.

    Attributes:
        codec (Codec | None): The codec used for encoding and decoding messages.
        compress_min_bytes (int): The minimum payload size (in bytes) before compression is applied.
        send_max_bytes (int): The maximum allowed size (in bytes) for a single message to be sent.
        compression (Compression | None): The compression algorithm to use, or None for no compression.
    """

    codec: Codec | None
    compress_min_bytes: int
    send_max_bytes: int
    compression: Compression | None

    def __init__(
        self, codec: Codec | None, compression: Compression | None, compress_min_bytes: int, send_max_bytes: int
    ) -> None:
        """Initializes the marshaler with the specified codec, compression settings, and byte limits.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding, or None if not specified.
            compression (Compression | None): The compression algorithm to use, or None if not specified.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes allowed to send in a single message.
        """
        self.codec = codec
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.compression = compression

    def marshal_end_stream(self, error: ConnectError | None, response_trailers: Headers) -> bytes:
        """Serializes the end-of-stream message for a Connect protocol response.

        This method converts the provided error (if any) and response trailers into a JSON object,
        encodes it, wraps it in an envelope with the end_stream flag, and returns the final bytes
        to be sent over the wire.

        Args:
            error (ConnectError | None): The error to include in the end-of-stream message, or None if no error occurred.
            response_trailers (Headers): The response trailers to include in the end-of-stream message.

        Returns:
            bytes: The serialized and enveloped end-of-stream message.
        """
        json_obj = end_stream_to_json(error, response_trailers)
        json_str = json.dumps(json_obj)

        env = self.write_envelope(json_str.encode(), EnvelopeFlags.end_stream)

        return env.encode()
