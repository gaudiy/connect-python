"""Module providing classes for unmarshaling unary and streaming Connect protocol messages."""

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
)
from typing import Any

from connect.code import Code
from connect.codec import Codec
from connect.compression import Compression
from connect.envelope import EnvelopeReader
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol_connect.end_stream import end_stream_from_bytes
from connect.utils import get_acallable_attribute


class ConnectUnaryUnmarshaler:
    """A class to handle the unmarshaling of data using a specified codec.

    Attributes:
        codec (Codec): The codec used for unmarshaling the data.
        body (bytes): The raw data to be unmarshaled.
        read_max_bytes (int): The maximum number of bytes to read.
        compression (Compression | None): The compression method to use, if any.

    """

    codec: Codec | None
    read_max_bytes: int
    compression: Compression | None
    stream: AsyncIterable[bytes] | None

    def __init__(
        self,
        codec: Codec | None,
        read_max_bytes: int,
        compression: Compression | None = None,
        stream: AsyncIterable[bytes] | None = None,
    ) -> None:
        """Initialize the ProtocolConnect object.

        Args:
            stream (AsyncIterable[bytes] | None): The stream of bytes to be unmarshaled.
            codec (Codec): The codec used for encoding/decoding the message.
            read_max_bytes (int): The maximum number of bytes to read.
            compression (Compression | None): The compression method to use, if any.

        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream

    async def unmarshal(self, message: Any) -> Any:
        """Asynchronously unmarshals a given message using the provided unmarshal function and codec.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The result of the unmarshaling process.

        """
        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        return await self.unmarshal_func(message, self.codec.unmarshal)

    async def unmarshal_func(self, message: Any, func: Callable[[bytes, Any], Any]) -> Any:
        """Asynchronously unmarshals a message using the provided function.

        This function reads data from the stream in chunks, checks if the total
        bytes read exceed the maximum allowed bytes, and optionally decompresses
        the data. It then uses the provided function to unmarshal the data into
        the desired format.

        Args:
            message (Any): The message to be unmarshaled.
            func (Callable[[bytes, Any], Any]): A function that takes the raw bytes
                and the message, and returns the unmarshaled object.

        Returns:
            Any: The unmarshaled object.

        Raises:
            ConnectError: If the stream is not set, if the message size exceeds the
                maximum allowed bytes, or if there is an error during unmarshaling.

        """
        if self.stream is None:
            raise ConnectError("stream is not set", Code.INTERNAL)

        chunks: list[bytes] = []
        bytes_read = 0
        try:
            async for chunk in self.stream:
                chunk_size = len(chunk)
                bytes_read += chunk_size
                if self.read_max_bytes > 0 and bytes_read > self.read_max_bytes:
                    raise ConnectError(
                        f"message size {bytes_read} is larger than configured max {self.read_max_bytes}",
                        Code.RESOURCE_EXHAUSTED,
                    )

                chunks.append(chunk)

            data = b"".join(chunks)

            if len(data) > 0 and self.compression:
                data = self.compression.decompress(data, self.read_max_bytes)

            try:
                obj = func(data, message)
            except Exception as e:
                raise ConnectError(
                    f"unmarshal message: {str(e)}",
                    Code.INVALID_ARGUMENT,
                ) from e
        finally:
            await self.aclose()

        return obj

    async def aclose(self) -> None:
        """Asynchronously close the stream if it is set.

        This method is intended to be called when the stream is no longer needed
        to release any associated resources.

        """
        aclose = get_acallable_attribute(self.stream, "aclose")
        if aclose:
            await aclose()


class ConnectStreamingUnmarshaler(EnvelopeReader):
    """A class to handle the unmarshaling of streaming data.

    Attributes:
        codec (Codec): The codec used for unmarshaling data.
        compression (Compression | None): The compression method used, if any.
        stream (AsyncIterable[bytes] | None): The asynchronous byte stream to read from.
        buffer (bytes): The buffer to store incoming data chunks.

    """

    _end_stream_error: ConnectError | None
    _trailers: Headers

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
        super().__init__(codec, read_max_bytes, stream, compression)
        self._end_stream_error = None
        self._trailers = Headers()

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
        async for obj, end in super().unmarshal(message):
            if self.last:
                error, trailers = end_stream_from_bytes(self.last.data)
                self._end_stream_error = error
                self._trailers = trailers

            yield obj, end

    @property
    def trailers(self) -> Headers:
        """Return the trailers headers.

        Trailers are additional headers sent after the body of the message.

        Returns:
            Headers: The trailers headers.

        """
        return self._trailers

    @property
    def end_stream_error(self) -> ConnectError | None:
        """Return the error that occurred at the end of the stream, if any.

        Returns:
            ConnectError | None: The error that occurred at the end of the stream,
            or None if no error occurred.

        """
        return self._end_stream_error
