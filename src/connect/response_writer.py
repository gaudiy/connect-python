"""Module containing the ServerResponseWriter class."""

import asyncio

from connect.response import Response


class ServerResponseWriter:
    """A writer class for handling server responses asynchronously using an asyncio.Queue.

    Attributes:
        queue (asyncio.Queue[Response]): The queue used to store a single response.
        is_closed (bool): Indicates whether the writer has been closed.

    """

    queue: asyncio.Queue[Response]
    is_closed: bool = False

    def __init__(self) -> None:
        """Initialize the instance with an asyncio queue of maximum size 1."""
        self.queue = asyncio.Queue(maxsize=1)

    async def write(self, response: Response) -> None:
        """Asynchronously writes a response to the internal queue.

        Args:
            response (Response): The response object to be written.

        Raises:
            RuntimeError: If the response writer is already closed.

        """
        if self.is_closed:
            raise RuntimeError("Cannot write to a closed response writer.")

        await self.queue.put(response)

    async def receive(self) -> Response:
        """Asynchronously retrieves a response from the internal queue.

        Raises:
            RuntimeError: If the response writer is already closed.

        Returns:
            Response: The next response item from the queue.

        Side Effects:
            Marks the response writer as closed after receiving a response.

        """
        if self.is_closed:
            raise RuntimeError("Cannot receive from a closed response writer.")

        response = await self.queue.get()
        self.queue.task_done()

        self.is_closed = True
        return response
