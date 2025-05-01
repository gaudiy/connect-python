"""Module containing the Envelope class which represents a data envelope."""

import struct
from collections.abc import AsyncIterable, AsyncIterator
from enum import Flag
from typing import Any

from connect.code import Code
from connect.codec import Codec
from connect.compression import Compression
from connect.error import ConnectError
from connect.utils import get_acallable_attribute


class EnvelopeFlags(Flag):
    """EnvelopeFlags is an enumeration that defines flags for an envelope.

    Attributes:
        compressed (int): Flag indicating that the envelope is compressed.
        end_stream (int): Flag indicating that the envelope marks the end of a stream.

    """

    compressed = 0b00000001
    end_stream = 0b00000010


class Envelope:
    """A class to represent an Envelope which contains data and flags.

    Attributes:
        data (bytes): The data contained in the envelope.
        flags (EnvelopeFlags): The flags associated with the envelope.
        _format (str): The format string used for struct packing and unpacking.

    """

    data: bytes
    flags: EnvelopeFlags
    _format: str = ">BI"

    def __init__(self, data: bytes, flags: EnvelopeFlags) -> None:
        """Initialize a new instance of the class.

        Args:
            data (bytes): The data to be processed.
            flags (EnvelopeFlags): The flags associated with the envelope.

        """
        self.data = data
        self.flags = flags

    def encode(self) -> bytes:
        """Encode the header and data into a byte sequence.

        Returns:
            bytes: The encoded byte sequence consisting of the header and data.

        """
        return self.encode_header(self.flags.value, self.data) + self.data

    def encode_header(self, flags: int, data: bytes) -> bytes:
        """Encode the header for a protocol message.

        Args:
            flags (int): The flags to include in the header.
            data (bytes): The data to be sent, used to determine the length.

        Returns:
            bytes: The encoded header as a byte string.

        """
        return struct.pack(self._format, flags, len(data))

    @staticmethod
    def decode_header(data: bytes) -> tuple[EnvelopeFlags, int] | None:
        """Decode the header from the given byte data.

        Args:
            data (bytes): The byte data containing the header to decode.

        Returns:
            tuple[EnvelopeFlags, int] | None: A tuple containing the decoded EnvelopeFlags and data length if the data is valid,
                                              otherwise None if the data length is less than 5 bytes.

        """
        if len(data) < 5:
            return None

        flags, data_len = struct.unpack(Envelope._format, data[:5])
        return EnvelopeFlags(flags), data_len

    @staticmethod
    def decode(data: bytes) -> "tuple[Envelope | None, int]":
        """Decode the given byte data into an Envelope object and its length.

        Args:
            data (bytes): The byte data to decode.

        Returns:
            tuple[Envelope | None, int]: A tuple containing the decoded Envelope object (or None if decoding fails)
            and the length of the data. If the data is insufficient to decode, returns (None, data_len).

        """
        header = Envelope.decode_header(data)
        if header is None:
            return None, 0

        flags, data_len = header
        if len(data) < 5 + data_len:
            return None, data_len

        return Envelope(data[5 : 5 + data_len], flags), data_len

    def is_set(self, flag: EnvelopeFlags) -> bool:
        """Check if a specific flag is set in the envelope.

        Args:
            flag (EnvelopeFlags): The flag to check.

        Returns:
            bool: True if the flag is set, False otherwise.

        """
        return flag in self.flags


class EnvelopeWriter:
    """EnvelopeWriter is responsible for marshaling messages, optionally compressing them, and writing them into envelopes for transmission.

    Attributes:
        codec (Codec | None): The codec used for encoding and decoding messages.
        send_max_bytes (int): The maximum number of bytes allowed per message.
        compression (Compression | None): The compression method to use, or None for no compression.

    Methods:
        __init__(codec, compression, compress_min_bytes, send_max_bytes):
            Initializes the EnvelopeWriter with the specified codec, compression, and size constraints.

        async _marshal(messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
            Asynchronously marshals and optionally compresses messages from an async iterable, yielding encoded envelope bytes.
            Raises ConnectError if marshaling fails or message size exceeds the allowed limit.

        write_envelope(data: bytes, flags: EnvelopeFlags) -> Envelope:
            Writes an envelope, optionally compressing its data if conditions are met, and updates envelope flags accordingly.
            Raises ConnectError if the (compressed) message size exceeds the allowed maximum.

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

    async def marshal(self, messages: AsyncIterable[Any]) -> AsyncIterator[bytes]:
        """Asynchronously marshals and compresses messages from an asynchronous iterator.

        Args:
            messages (AsyncIterable[Any]): An asynchronous iterable of messages to be marshaled.

        Yields:
            AsyncIterator[bytes]: An asynchronous iterator of marshaled and optionally compressed messages in bytes.

        Raises:
            ConnectError: If there is an error during marshaling or if the message size exceeds the allowed limit.

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
        """Write an envelope containing the provided data, applying compression if required.

        Args:
            data (bytes): The message payload to be written into the envelope.
            flags (EnvelopeFlags): Flags indicating envelope properties, such as compression.

        Returns:
            Envelope: An envelope object containing the (optionally compressed) data and updated flags.

        Raises:
            ConnectError: If the (compressed or uncompressed) data size exceeds the configured send_max_bytes limit.

        Notes:
            - Compression is applied only if the flags do not already indicate compression,
              compression is enabled, and the data size exceeds the minimum threshold.
            - The flags are updated to include the compressed flag if compression is performed.

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

            if self.send_max_bytes > 0 and len(data) > self.send_max_bytes:
                raise ConnectError(
                    f"compressed message size {len(data)} exceeds send_max_bytes {self.send_max_bytes}",
                    Code.RESOURCE_EXHAUSTED,
                )

        return Envelope(
            data=compressed_data,
            flags=flags,
        )


class EnvelopeReader:
    """A class to handle the unmarshaling of streaming data.

    Attributes:
        codec (Codec): The codec used for unmarshaling data.
        compression (Compression | None): The compression method used, if any.
        stream (AsyncIterable[bytes] | None): The asynchronous byte stream to read from.
        buffer (bytes): The buffer to store incoming data chunks.

    """

    codec: Codec | None
    read_max_bytes: int
    compression: Compression | None
    stream: AsyncIterable[bytes] | None
    buffer: bytes
    last_data: bytes | None

    def __init__(
        self,
        codec: Codec | None,
        read_max_bytes: int,
        stream: AsyncIterable[bytes] | None = None,
        compression: Compression | None = None,
    ) -> None:
        """Initialize the protocol connection.

        Args:
            codec (Codec): The codec to use for encoding and decoding data.
            read_max_bytes (int): The maximum number of bytes to read from the stream.
            stream (AsyncIterable[bytes] | None, optional): The asynchronous byte stream to read from. Defaults to None.
            compression (Compression | None, optional): The compression method to use. Defaults to None.

        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream
        self.buffer = b""
        self.last_data = None

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Asynchronously unmarshals messages from the stream.

        Args:
            message (Any): The message type to unmarshal.

        Yields:
            Any: The unmarshaled message object.

        Raises:
            ConnectError: If the stream is not set, if there is an error in the
                          unmarshaling process, or if there is a protocol error.

        """
        if self.stream is None:
            raise ConnectError("stream is not set", Code.INTERNAL)

        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        async for chunk in self.stream:
            self.buffer += chunk

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
                    self.last_data = env.data
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
        """Asynchronously closes the stream if it has an `aclose` method.

        This method checks if the `self.stream` object has an asynchronous
        `aclose` method. If the method exists, it is invoked to close the stream.

        Returns:
            None

        """
        aclose = get_acallable_attribute(self.stream, "aclose")
        if aclose:
            await aclose()
