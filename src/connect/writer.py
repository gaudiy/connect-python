import asyncio

from connect.response import Response


class ServerResponseWriter:
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue[Response]
    stop_event: asyncio.Event

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.queue = asyncio.Queue(maxsize=1)
        # self.stop_event = asyncio.Event()

        # self.worker_task = self.loop.create_task(self.send())

    async def write(self, response: Response) -> None:
        await self.queue.put(response)

    async def receive(self) -> Response:
        return await self.queue.get()

    # async def send(self) -> Response:
    #     try:
    #         while not self.stop_event.is_set():
    #             try:
    #                 res = await asyncio.wait_for(self.queue.get(), timeout=0.1)
    #             except TimeoutError:
    #                 continue

    #             self.queue.task_done()
    #             return res
    #     except asyncio.CancelledError:
    #         pass

    #     return Response()

    # async def close(self) -> None:
    #     await self.queue.join()

    #     if not self.stop_event.is_set():
    #         self.stop_event.set()

    #     if not self.worker_task.done():
    #         self.worker_task.cancel()

    #     if self.worker_task:
    #         done, _ = await asyncio.wait(
    #             [self.worker_task],
    #             return_when=asyncio.ALL_COMPLETED,
    #         )
    #         for d in done:
    #             exc = d.exception()
    #             if exc and not isinstance(exc, asyncio.CancelledError):
    #                 logger.error(f"Task raised an exception: {exc}")

    #     if not self.queue.empty():
    #         logger.warning("Response queue is not empty")
