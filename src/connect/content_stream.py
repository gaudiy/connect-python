"""Asynchronous byte stream utilities for HTTP core response handling."""

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
    """An abstract base class for asynchronous byte streams.

    This class defines the interface for an asynchronous byte stream, which
    includes methods for iterating over the stream and closing it.

    """

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronous iterator method.

        This method should be implemented to provide asynchronous iteration
        over the object. It must return an asynchronous iterator that yields
        bytes.

        Raises:
            NotImplementedError: If the method is not implemented.

        """
        raise NotImplementedError("The '__aiter__' method must be implemented.")  # pragma: no cover
        yield b""

    async def aclose(self) -> None:
        """Asynchronously close the byte stream."""
        pass


class BoundAsyncStream(AsyncByteStream):
    """An asynchronous byte stream wrapper that binds to an existing async iterable of bytes.

    This class provides an asynchronous iterator interface for reading byte chunks from the given stream,
    and ensures proper resource cleanup by closing the underlying stream when needed.

    Args:
        stream (AsyncIterable[bytes]): The asynchronous iterable byte stream to wrap.

    Attributes:
        stream (AsyncIterable[bytes]): The wrapped asynchronous byte stream.
        _closed (bool): Indicates whether the stream has been closed.

    """

    _stream: AsyncIterable[bytes] | None
    _closed: bool

    def __init__(self, stream: AsyncIterable[bytes]) -> None:
        """Initialize the object with an asynchronous iterable stream of bytes.

        Args:
            stream (AsyncIterable[bytes]): An asynchronous iterable that yields bytes.

        """
        self._stream = stream
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronous iterator method to read byte chunks from the stream."""
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
        """Asynchronously close the stream."""
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
    """An asynchronous data stream wrapper that provides iteration and cleanup functionality.

    Type Parameters:
        T: The type of items yielded by the stream.

    Attributes:
        _stream (AsyncIterable[T]): The underlying asynchronous iterable data stream.
        aclose_func (Callable[..., Awaitable[None]] | None): Optional asynchronous cleanup function to be called on close.

    """

    _stream: AsyncIterable[T] | None
    _aclose_func: Callable[..., Awaitable[None]] | None
    _closed: bool

    def __init__(self, stream: AsyncIterable[T], aclose_func: Callable[..., Awaitable[None]] | None = None) -> None:
        """Initialize the object with an asynchronous iterable stream and an optional asynchronous close function.

        Args:
            stream (AsyncIterable[T]): The asynchronous iterable stream to be wrapped.
            aclose_func (Callable[..., Awaitable[None]], optional): An optional asynchronous function to be called when closing the stream. Defaults to None.

        """
        self._stream = stream
        self._aclose_func = aclose_func
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[T]:
        """Asynchronously iterates over the underlying stream, yielding each part.

        Yields:
            T: The next part from the stream.

        Raises:
            Propagates any exception raised during iteration after ensuring the stream is closed.

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
        """Asynchronously closes the underlying stream.

        If a custom asynchronous close function (`aclose_func`) is provided, it is awaited.
        Otherwise, if the underlying stream has an `aclose` method, it is retrieved and awaited.

        Raises:
            Any exception raised by the custom close function or the stream's `aclose` method.

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
