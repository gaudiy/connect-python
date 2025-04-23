from __future__ import annotations

import typing
from functools import partial
from typing import Any

import anyio
from starlette._utils import collapse_excgroups  # type: ignore
from starlette.background import BackgroundTask
from starlette.concurrency import iterate_in_threadpool
from starlette.requests import ClientDisconnect
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

ContentStream = typing.Iterable[typing.Any] | typing.AsyncIterable[typing.Any]
AsyncContentStream = typing.AsyncIterable[typing.Any]


class StreamingResponseWithTrailers(Response):
    body_iterator: AsyncContentStream

    def __init__(
        self,
        content: ContentStream,
        *,
        status_code: int = 200,
        headers: typing.Mapping[str, str] | None = None,
        trailers: typing.Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        if isinstance(content, typing.AsyncIterable):
            self.body_iterator = content
        else:
            self.body_iterator = iterate_in_threadpool(content)

        self.status_code = status_code
        self.media_type = self.media_type if media_type is None else media_type
        self.background = background
        self.init_headers(headers)
        self._trailers = trailers

        if self._trailers:
            names = ", ".join({k for k, _ in self._trailers.items()})
            if names:
                self.headers.setdefault("Trailer", names)

    async def _stream_response(self, send: Send, trailers_supported: bool) -> None:
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
            "trailers": self._trailers is not None and trailers_supported,
        })

        async for chunk in self.body_iterator:
            if not isinstance(chunk, bytes | memoryview):
                chunk = chunk.encode(self.charset)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        await send({"type": "http.response.body", "body": b"", "more_body": False})

        if self._trailers is not None and trailers_supported:
            encoded_headers = [(key.encode(), value.encode()) for key, value in self._trailers.items()]
            await send({
                "type": "http.response.trailers",
                "headers": encoded_headers,
                "more_trailers": False,
            })

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        spec_version = tuple(map(int, scope.get("asgi", {}).get("spec_version", "2.0").split(".")))
        trailers_supported = "http.response.trailers" in scope.get("extensions", {})

        if spec_version >= (2, 4):
            try:
                await self._stream_response(send, trailers_supported)
            except OSError:
                raise ClientDisconnect() from None

        else:

            async def listen_for_disconnect() -> None:
                while True:
                    if (await receive())["type"] == "http.disconnect":
                        break

            with collapse_excgroups():
                async with anyio.create_task_group() as tg:

                    async def run_and_cancel(func: Any) -> None:
                        await func()
                        tg.cancel_scope.cancel()

                    tg.start_soon(
                        run_and_cancel,
                        partial(self._stream_response, send, trailers_supported),
                    )
                    await run_and_cancel(listen_for_disconnect)

        if self.background is not None:
            await self.background()
