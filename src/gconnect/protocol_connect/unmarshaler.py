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

"""Connect protocol message unmarshaling utilities."""

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
)
from typing import Any

from gconnect.code import Code
from gconnect.codec import Codec
from gconnect.compression import Compression
from gconnect.envelope import EnvelopeReader
from gconnect.error import ConnectError
from gconnect.headers import Headers
from gconnect.protocol_connect.end_stream import end_stream_from_bytes
from gconnect.utils import get_acallable_attribute


class ConnectUnaryUnmarshaler:
    """ConnectUnaryUnmarshaler is responsible for asynchronously reading and unmarshaling messages from an async byte stream.

    This class manages the process of reading data in chunks from an asynchronous stream, enforcing a maximum read size,
    optionally decompressing the data, and then decoding (unmarshaling) the message using a provided codec. It also ensures
    proper cleanup of the stream resource.

    Attributes:
        codec (Codec | None): The codec used for encoding/decoding the message.
        read_max_bytes (int): The maximum number of bytes to read from the stream.
        compression (Compression | None): The compression method to use, if any.
        stream (AsyncIterable[bytes] | None): The asynchronous stream of bytes to be unmarshaled.
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
        """Initializes the object with the specified codec, maximum read bytes, compression method, and optional asynchronous byte stream.

        Args:
            codec (Codec | None): The codec to use for decoding, or None if not specified.
            read_max_bytes (int): The maximum number of bytes to read at once.
            compression (Compression | None, optional): The compression method to use, or None for no compression. Defaults to None.
            stream (AsyncIterable[bytes] | None, optional): An optional asynchronous iterable byte stream. Defaults to None.
        """
        self.codec = codec
        self.read_max_bytes = read_max_bytes
        self.compression = compression
        self.stream = stream

    async def unmarshal(self, message: Any) -> Any:
        """Asynchronously unmarshals a given message using the configured codec.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled message.

        Raises:
            ConnectError: If the codec is not set.
        """
        if self.codec is None:
            raise ConnectError("codec is not set", Code.INTERNAL)

        return await self.unmarshal_func(message, self.codec.unmarshal)

    async def unmarshal_func(self, message: Any, func: Callable[[bytes, Any], Any]) -> Any:
        """Asynchronously reads data from the stream, optionally decompresses it, and applies a given function to unmarshal the data.

        Args:
            message (Any): The message context or object to be passed to the unmarshal function.
            func (Callable[[bytes, Any], Any]): A callable that takes the raw bytes and the message, and returns the unmarshaled object.

        Returns:
            Any: The result of the unmarshal function applied to the data and message.

        Raises:
            ConnectError: If the stream is not set, if the message size exceeds the configured maximum, or if unmarshaling fails.

        Notes:
            - The stream is closed after processing, regardless of success or failure.
            - If compression is enabled, the data is decompressed before unmarshaling.
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
        """Asynchronously closes the underlying stream if it supports asynchronous closing.

        This method attempts to retrieve an asynchronous close method (`aclose`) from the
        `stream` attribute using the `get_acallable_attribute` utility. If such a method exists,
        it is awaited to properly close the stream and release any associated resources.

        Raises:
            Any exception raised by the underlying stream's `aclose` method.
        """
        aclose = get_acallable_attribute(self.stream, "aclose")
        if aclose:
            await aclose()


class ConnectStreamingUnmarshaler(EnvelopeReader):
    """ConnectStreamingUnmarshaler is an asynchronous envelope reader for streaming Connect protocol messages.

    This class is responsible for reading, decoding, and handling streamed messages from an asynchronous byte stream,
    optionally applying compression and decoding using a specified codec. It also manages end-of-stream errors and
    trailer headers, which are additional headers sent after the message body.

    Attributes:
        _end_stream_error (ConnectError | None): Stores any error that occurred at the end of the stream.
        _trailers (Headers): Stores the trailers headers received at the end of the stream.
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
        """Initializes the object with the specified codec, maximum read bytes, optional stream, and optional compression.

        Args:
            codec (Codec | None): The codec to use for decoding, or None.
            read_max_bytes (int): The maximum number of bytes to read.
            stream (AsyncIterable[bytes] | None, optional): An optional asynchronous byte stream. Defaults to None.
            compression (Compression | None, optional): The compression method to use, or None. Defaults to None.
        """
        super().__init__(codec, read_max_bytes, stream, compression)
        self._end_stream_error = None
        self._trailers = Headers()

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Asynchronously unmarshals a message, yielding objects and end-of-stream flags.

        Iterates over the result of the superclass's `unmarshal` method, yielding each
        object and a boolean indicating if it is the end of the stream. If `self.last`
        is set, extracts error and trailer information from its data and stores them
        in instance variables.

        Args:
            message (Any): The message to be unmarshaled.

        Yields:
            tuple[Any, bool]: A tuple containing the unmarshaled object and a boolean
                indicating if it is the end of the stream.
        """
        async for obj, end in super().unmarshal(message):
            if self.last:
                error, trailers = end_stream_from_bytes(self.last.data)
                self._end_stream_error = error
                self._trailers = trailers

            yield obj, end

    @property
    def trailers(self) -> Headers:
        """Returns the trailers associated with the response.

        Trailers are additional headers sent after the response body, typically used in protocols like HTTP/2 or gRPC to provide metadata at the end of a message.

        Returns:
            Headers: The trailers as a Headers object.
        """
        return self._trailers

    @property
    def end_stream_error(self) -> ConnectError | None:
        """Returns the error that occurred at the end of the stream, if any.

        Returns:
            ConnectError | None: The error encountered at the end of the stream, or None if no error occurred.
        """
        return self._end_stream_error
