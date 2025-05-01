from collections.abc import (
    AsyncIterable,
    AsyncIterator,
)

from connect.utils import (
    AsyncByteStream,
    get_acallable_attribute,
    map_httpcore_exceptions,
)


class HTTPCoreResponseAsyncByteStream(AsyncByteStream):
    """An asynchronous byte stream for reading and writing byte chunks."""

    aiterator: AsyncIterable[bytes] | None
    _closed: bool

    def __init__(
        self,
        aiterator: AsyncIterable[bytes] | None = None,
    ) -> None:
        """Initialize the protocol connect instance.

        Args:
            aiterator (AsyncIterable[bytes] | None): An optional asynchronous iterable of bytes.

        Returns:
            None

        """
        self.aiterator = aiterator
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronous iterator method to read byte chunks from the stream."""
        if self.aiterator:
            try:
                with map_httpcore_exceptions():
                    async for chunk in self.aiterator:
                        yield chunk
            except BaseException as exc:
                await self.aclose()
                raise exc

    async def aclose(self) -> None:
        """Asynchronously close the stream."""
        if not self._closed and self.aiterator:
            aclose = get_acallable_attribute(self.aiterator, "aclose")
            if not aclose:
                return

            with map_httpcore_exceptions():
                await aclose()
