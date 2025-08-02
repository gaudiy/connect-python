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

"""Module containing the Envelope class which represents a data envelope."""

import struct
from collections.abc import AsyncIterable, AsyncIterator
from enum import Flag
from typing import Any

from gconnect.code import Code
from gconnect.codec import Codec
from gconnect.compression import Compression
from gconnect.error import ConnectError
from gconnect.utils import get_acallable_attribute


class EnvelopeFlags(Flag):
    """Flags for an envelope.

    This enumeration defines the bit flags that can be set on a Connect protocol
    envelope to indicate special handling or metadata.

    Attributes:
        compressed: Indicates that the message is compressed. The compression
            algorithm is determined by the `Content-Encoding` header.
        end_stream: Signals the end of a stream. This is used in streaming RPCs
            to indicate that no more messages will be sent.
        trailer: Indicates that the envelope contains trailers instead of a
            message. Trailers are sent as the last message in a stream.
    """

    compressed = 0b00000001
    end_stream = 0b00000010
    trailer = 0b10000000


class Envelope:
    """A class to represent a protocol message envelope.

    This class handles the encoding and decoding of messages, which consist of a
    5-byte header and a variable-length data payload. The header contains flags
    and the length of the data payload. The structure of the header is defined
    by the `_format` attribute, which is a struct format string '>BI'
    (big-endian, 1-byte unsigned char for flags, 4-byte unsigned int for data length).

    Attributes:
        data (bytes): The payload of the envelope.
        flags (EnvelopeFlags): An enum representing the flags associated with the envelope.
        _format (str): The struct format string for encoding/decoding the header.
    """

    data: bytes
    flags: EnvelopeFlags
    _format: str = ">BI"

    def __init__(self, data: bytes, flags: EnvelopeFlags) -> None:
        """Initializes a new Envelope instance.

        Args:
            data: The raw byte data of the envelope.
            flags: The flags associated with the envelope, indicating its type.
        """
        self.data = data
        self.flags = flags

    def encode(self) -> bytes:
        """Serializes the envelope into a byte representation.

        The resulting byte string is a concatenation of the message header
        and the message data. The header contains the flags and the length
        of the data.

        Returns:
            The serialized envelope as a bytes object.
        """
        return self.encode_header(self.flags.value, self.data) + self.data

    def encode_header(self, flags: int, data: bytes) -> bytes:
        """Encodes the header for a message envelope.

        This method packs the given flags and the length of the data into a
        binary structure according to the format defined in `self._format`.

        Args:
            flags: An integer representing the message flags.
            data: The byte string payload of the message. The length of this
                data will be encoded in the header.

        Returns:
            The encoded header as a byte string.
        """
        return struct.pack(self._format, flags, len(data))

    @staticmethod
    def decode_header(data: bytes) -> tuple[EnvelopeFlags, int] | None:
        """Decodes an envelope header from a byte string.

        This function reads the first 5 bytes of the provided data to extract
        the envelope flags and the length of the main data payload.

        Args:
            data: The byte string containing the envelope header.

        Returns:
            A tuple containing the `EnvelopeFlags` and the data length as an
            integer if the header is successfully decoded. Returns `None` if

            the input data is too short to contain a valid header (i.e., less
            than 5 bytes).
        """
        if len(data) < 5:
            return None

        flags, data_len = struct.unpack(Envelope._format, data[:5])
        return EnvelopeFlags(flags), data_len

    @staticmethod
    def decode(data: bytes) -> "tuple[Envelope | None, int]":
        """Decodes a byte stream into an Envelope object.

        This method reads the envelope header to determine the payload size,
        then attempts to construct an Envelope object from the payload.

        Args:
            data: The raw byte data to be decoded.

        Returns:
            A tuple containing the decoded Envelope and the payload length.
            - If decoding is successful, returns `(Envelope, payload_length)`.
            - If the data is insufficient to contain the full payload as
              indicated by the header, returns `(None, expected_payload_length)`.
            - If the header is invalid or cannot be decoded, returns `(None, 0)`.
        """
        header = Envelope.decode_header(data)
        if header is None:
            return None, 0

        flags, data_len = header
        if len(data) < 5 + data_len:
            return None, data_len

        return Envelope(data[5 : 5 + data_len], flags), data_len

    def is_set(self, flag: EnvelopeFlags) -> bool:
        """Checks if a specific flag is set in the envelope's flags.

        Args:
            flag: The flag to check for.

        Returns:
            True if the flag is set, False otherwise.
        """
        return flag in self.flags


class EnvelopeWriter:
    """Manages the process of marshaling, compressing, and framing messages into envelopes.

    This class is responsible for taking application-level messages, encoding them into
    bytes using a specified codec, and then optionally compressing them. The resulting
    data is wrapped in an Envelope object, which includes flags and the payload,
    ready for transmission. It also enforces size limits on outgoing messages.

    Attributes:
        codec (Codec | None): The codec used for marshaling messages.
        compress_min_bytes (int): The minimum size in bytes a message must be
            before compression is applied.
        send_max_bytes (int): The maximum allowed size in bytes for a message
            payload after any compression.
        compression (Compression | None): The compression algorithm to use. If None,
            compression is disabled.
    """

    codec: Codec | None
    compress_min_bytes: int
    send_max_bytes: int
    compression: Compression | None

    def __init__(
        self, codec: Codec | None, compression: Compression | None, compress_min_bytes: int, send_max_bytes: int
    ) -> None:
        """Initializes the Envelope.

        Args:
            codec: The codec to use for encoding messages.
            compression: The compression algorithm to use.
            compress_min_bytes: The minimum number of bytes a message must be to be compressed.
            send_max_bytes: The maximum number of bytes for a message to be sent.
        """
        self.codec = codec
        self.compress_min_bytes = compress_min_bytes
        self.send_max_bytes = send_max_bytes
        self.compression = compression

    async def marshal(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Marshals an asynchronous stream of messages into Connect envelopes.

        This asynchronous generator takes an iterable of messages, marshals each one
        using the configured codec, wraps it in a Connect envelope, and yields
        the encoded envelope as bytes.

        Args:
            messages: An asynchronous iterable of messages to be marshaled.

        Yields:
            The next marshaled and enveloped message as bytes.

        Raises:
            ConnectError: If the codec is not set or if an error occurs
                during message marshaling.
        """
        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        async for message in messages:
            try:
                data = self.codec.marshal(message)
            except Exception as e:
                raise ConnectError(f"marshal message: {str(e)}", Code.INTERNAL) from e

            env = self.write_envelope(data, EnvelopeFlags(0))
            yield env.encode()

    def write_envelope(self, data: bytes, flags: EnvelopeFlags) -> Envelope:
        """Creates an Envelope from the given data, handling compression.

        This method takes raw byte data and prepares it for sending. It will
        attempt to compress the data if a compression algorithm is configured,
        the data is larger than `compress_min_bytes`, and the `compressed`
        flag is not already set.

        If the data is compressed, the `EnvelopeFlags.compressed` flag is added.
        The method also validates the final data size against the `send_max_bytes`
        limit, raising an error if it's exceeded.

        Args:
            data (bytes): The raw message data to be enveloped.
            flags (EnvelopeFlags): The initial flags for the envelope.

        Returns:
            Envelope: An envelope containing the potentially compressed data and
                updated flags.

        Raises:
            ConnectError: If the size of the data (either raw or compressed)
                exceeds the configured `send_max_bytes` limit.
        """
        if EnvelopeFlags.compressed in flags or self.compression is None or len(data) < self.compress_min_bytes:
            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                raise ConnectError(
                    f"message size {len(data)} exceeds sendMaxBytes {self.send_max_bytes}", Code.RESOURCE_EXHAUSTED
                )
            compressed_data = data
            flags = flags
        else:
            compressed_data = self.compression.compress(data)
            flags |= EnvelopeFlags.compressed

            if self.send_max_bytes > 0 and len(compressed_data) > self.send_max_bytes:
                raise ConnectError(
                    f"compressed message size {len(compressed_data)} exceeds send_max_bytes {self.send_max_bytes}",
                    Code.RESOURCE_EXHAUSTED,
                )

        return Envelope(
            data=compressed_data,
            flags=flags,
        )


class EnvelopeReader:
    """Reads and decodes enveloped messages from an asynchronous byte stream.

    This class is responsible for processing the Connect protocol's envelope format.
    It reads data from a stream, parses envelopes (which consist of a flag byte,
    a 4-byte length prefix, and the message data), handles decompression, and
    uses a specified codec to unmarshal the message data into Python objects.

    Attributes:
        codec (Codec | None): The codec used for unmarshaling message data.
        read_max_bytes (int): The maximum permitted size in bytes for a single message.
        compression (Compression | None): The algorithm used for decompressing message data.
        stream (AsyncIterable[bytes] | None): The source asynchronous byte stream.
        buffer (bytes): An internal buffer for accumulating data from the stream.
        bytes_read (int): A counter for the total number of bytes read.
        last (Envelope | None): Stores the final envelope, which typically contains
            end-of-stream metadata.
    """

    codec: Codec | None
    read_max_bytes: int
    compression: Compression | None
    stream: AsyncIterable[bytes] | None
    buffer: bytes
    bytes_read: int
    last: Envelope | None

    def __init__(
        self,
        codec: Codec | None,
        read_max_bytes: int,
        stream: AsyncIterable[bytes] | None = None,
        compression: Compression | None = None,
    ) -> None:
        """Initializes the EnvelopeReader.

        Args:
            codec: The codec to use for decoding messages.
            read_max_bytes: The maximum number of bytes to read from the stream.
            stream: The asynchronous stream of bytes to read from.
            compression: The compression algorithm to use for decompression.
        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream
        self.buffer = b""
        self.bytes_read = 0
        self.last = None

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Unmarshals a stream of enveloped messages according to the Connect protocol.

        This asynchronous generator reads byte chunks from the underlying stream,
        buffering them until a complete message envelope can be decoded. It handles
        message framing, decompression, and unmarshaling of the payload.

        Args:
            message (Any): The target message type (e.g., a protobuf message class)
                into which the payload will be unmarshaled.

        Yields:
            tuple[Any, bool]: An async iterator yielding tuples where the first element
                is the unmarshaled message object and the second is a boolean flag.
                The flag is `True` if this is the final message (i.e., an end-of-stream
                envelope), otherwise `False`.

        Raises:
            ConnectError: If the stream or codec is not configured, if a message
                size exceeds the configured `read_max_bytes`, if a compressed
                message is received without a configured decompressor, or if any
                other protocol, decompression, or unmarshaling error occurs.
        """
        if self.stream is None:
            raise ConnectError("stream is not set", Code.INTERNAL)

        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        async for chunk in self.stream:
            self.buffer += chunk
            self.bytes_read += len(chunk)

            while True:
                env, data_len = Envelope.decode(self.buffer)
                if env is None:
                    break

                if self.read_max_bytes > 0 and data_len > self.read_max_bytes:
                    raise ConnectError(
                        f"message size {data_len} is larger than configured readMaxBytes {self.read_max_bytes}",
                        Code.RESOURCE_EXHAUSTED,
                    )

                self.buffer = self.buffer[5 + data_len :]

                if env.is_set(EnvelopeFlags.compressed):
                    if not self.compression:
                        raise ConnectError(
                            "protocol error: sent compressed message without compression support", Code.INTERNAL
                        )

                    env.data = self.compression.decompress(env.data, self.read_max_bytes)

                if env.flags != EnvelopeFlags(0) and env.flags != EnvelopeFlags.compressed:
                    self.last = env
                    end = True
                    obj = None
                else:
                    try:
                        obj = self.codec.unmarshal(env.data, message)
                    except Exception as e:
                        raise ConnectError(
                            f"unmarshal message: {str(e)}",
                            Code.INVALID_ARGUMENT,
                        ) from e

                    end = False

                yield obj, end

        if len(self.buffer) > 0:
            header = Envelope.decode_header(self.buffer)
            if header:
                message = (
                    f"protocol error: promised {header[1]} bytes in enveloped message, got {len(self.buffer) - 5} bytes"
                )
                raise ConnectError(message, Code.INVALID_ARGUMENT)

    async def aclose(self) -> None:
        """Asynchronously closes the underlying stream.

        This method checks for an `aclose` callable on the stream
        and awaits it if found, ensuring proper resource cleanup in an
        asynchronous environment.
        """
        aclose = get_acallable_attribute(self.stream, "aclose")
        if aclose:
            await aclose()
