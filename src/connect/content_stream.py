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

"""Provides classes for handling asynchronous content and data streams."""

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
)

from connect.utils import (
    get_acallable_attribute,
    map_httpcore_exceptions,
)


class AsyncByteStream(AsyncIterable[bytes]):
    """Abstract base class for asynchronous byte streams.

    This class defines the interface for an asynchronous iterable that yields bytes.
    It is intended to be subclassed to implement specific byte stream sources,
    such as file I/O, network connections, or in-memory buffers.

    Subclasses must implement the `__aiter__` method to provide the core
    asynchronous iteration logic. The `aclose` method can be overridden to
    release any underlying resources.
    """

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronously iterates over the content stream.

        This allows the object to be used in an `async for` loop, yielding
        the content in chunks.

        Yields:
            bytes: A chunk of the content from the stream.

        Returns:
            AsyncIterator[bytes]: An asynchronous iterator over the content stream.
        """
        raise NotImplementedError("The '__aiter__' method must be implemented.")  # pragma: no cover
        yield b""

    async def aclose(self) -> None:
        """Closes the stream and the underlying connection.

        This method is an asynchronous generator and should be used with `async for`.
        It will close the stream and the underlying connection when the generator is exhausted.
        """
        pass


class BoundAsyncStream(AsyncByteStream):
    """A wrapper for an asynchronous byte stream that ensures proper resource management.

    This class takes an asynchronous iterable of bytes and provides an `AsyncByteStream`
    interface. It is responsible for iterating over the underlying stream and ensuring
    that it is properly closed, even in the event of an error during iteration.

    The `aclose` method is idempotent, meaning it can be called multiple times without
    causing an error.

    Attributes:
        _stream (AsyncIterable[bytes] | None): The underlying asynchronous stream.
            It is set to `None` once the stream is closed.
        _closed (bool): A flag to indicate whether the stream has been closed.
    """

    _stream: AsyncIterable[bytes] | None
    _closed: bool

    def __init__(self, stream: AsyncIterable[bytes]) -> None:
        """Initialize the content stream.

        Args:
            stream: An asynchronous iterable of bytes representing the content stream.
        """
        self._stream = stream
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronously iterates over the response content.

        This method allows the response body to be consumed in chunks, which is
        useful for handling large files or streaming data. It ensures that the
        underlying stream is closed, even if an error occurs during iteration.

        Yields:
            bytes: A chunk of the response body.

        Raises:
            Exception: An exception that occurred during streaming. The stream
                is closed before the exception is re-raised.
            ExceptionGroup: Raised if an error occurs during iteration and a
                separate error occurs while attempting to close the stream.
        """
        if self._stream is None:
            return

        try:
            with map_httpcore_exceptions():
                async for chunk in self._stream:
                    yield chunk
        except Exception as exc:
            try:
                await self.aclose()
            except Exception as close_exc:
                raise ExceptionGroup("Multiple errors occurred", [exc, close_exc]) from exc
            raise

    async def aclose(self) -> None:
        """Asynchronously closes the content stream.

        This method ensures that the underlying stream is properly closed and
        resources are released. It is idempotent, meaning it can be called
        e   multiple times without raising an error or causing issues.

        Any exceptions that occur during the closing of the underlying `httpcore`
        stream are caught and re-raised as `connect.exceptions.ConnectError`.
        """
        if self._closed:
            return

        self._closed = True
        try:
            if self._stream is not None:
                with map_httpcore_exceptions():
                    aclose = get_acallable_attribute(self._stream, "aclose")
                    if aclose:
                        await aclose()
        finally:
            self._stream = None


class AsyncDataStream[T]:
    """Wraps an asynchronous iterable to provide a uniform interface for iteration and closure.

    This class is designed to handle various asynchronous data sources, such as streaming
    API responses, ensuring that the underlying resources are properly released after
    consumption or in case of an error.

    It can be used directly in an `async for` loop. The `aclose()` method should be
    called explicitly to ensure the stream is closed and resources are released.

    Type Parameters:
        T: The type of items yielded by the stream.

    Attributes:
        _stream (AsyncIterable[T] | None): The underlying asynchronous iterable.
        _aclose_func (Callable[..., Awaitable[None]] | None): An optional custom close function.
        _closed (bool): A flag indicating whether the stream has been closed.
    """

    _stream: AsyncIterable[T] | None
    _aclose_func: Callable[..., Awaitable[None]] | None
    _closed: bool

    def __init__(self, stream: AsyncIterable[T], aclose_func: Callable[..., Awaitable[None]] | None = None) -> None:
        """Initializes the ContentStream.

        Args:
            stream: The asynchronous iterable that provides the content.
            aclose_func: An optional asynchronous function to call when closing the stream.
        """
        self._stream = stream
        self._aclose_func = aclose_func
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[T]:
        """Asynchronously iterates over the content stream.

        This method allows the content stream to be used in an `async for` loop,
        yielding each part of the stream as it is received.

        Yields:
            T: The next part of the content from the stream.

        Raises:
            Exception: Re-raises any exception encountered during stream iteration
                after attempting to close the stream.
            ExceptionGroup: Raised if an exception occurs during stream iteration
                and another exception occurs while attempting to close the stream
                in response to the first error.
        """
        if self._stream is None:
            return

        try:
            async for part in self._stream:
                yield part
        except Exception as exc:
            try:
                await self.aclose()
            except Exception as close_exc:
                raise ExceptionGroup("Multiple errors occurred", [exc, close_exc]) from exc
            raise

    async def aclose(self) -> None:
        """Asynchronously closes the content stream and releases its resources.

        This method marks the stream as closed to prevent further operations.
        It will invoke the custom `_aclose_func` if one was provided during
        initialization. Otherwise, it attempts to call the `aclose()` method
        on the underlying stream object.

        The method is idempotent, meaning calling it on an already closed
        stream will have no effect. Finally, it clears internal references
        to the stream and the close function.
        """
        if self._closed:
            return

        self._closed = True
        try:
            if self._aclose_func:
                await self._aclose_func()
            elif self._stream is not None:
                aclose = get_acallable_attribute(self._stream, "aclose")
                if aclose:
                    await aclose()
        finally:
            self._stream = None
            self._aclose_func = None
