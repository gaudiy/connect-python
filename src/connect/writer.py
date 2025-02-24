"""Module containing the ServerResponseWriter class."""

import asyncio

from connect.response import Response


class ServerResponseWriter:
    """A class to handle writing and receiving server responses asynchronously.

    Attributes:
        _future (asyncio.Future[Response]): A future object to hold the server response.

    """

    _future: asyncio.Future[Response]

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Initialize the writer instance.

        Args:
            loop (asyncio.AbstractEventLoop | None, optional): The event loop to use. If None, the default event loop is used.

        Returns:
            None

        """
        loop = loop or asyncio.get_event_loop()
        self._future = loop.create_future()

    async def write(self, response: Response) -> None:
        """Asynchronously writes the given response to the future result if it is not already done.

        Args:
            response (Response): The response object to be written.

        Returns:
            None

        """
        if self._future.cancelled():
            raise RuntimeError("Cannot write response; the future has already been cancelled.")

        if self._future.done():
            raise RuntimeError("Cannot write response; the future is already done.")

        self._future.set_result(response)

    async def receive(self) -> Response:
        """Asynchronously receives a response.

        This method awaits the completion of a future and returns the response.

        Returns:
            Response: The response object awaited from the future.

        """
        return await self._future

    async def cancel(self) -> None:
        """Cancel the future if it is not already done.

        Returns:
            None

        """
        if not self._future.done():
            self._future.cancel()
