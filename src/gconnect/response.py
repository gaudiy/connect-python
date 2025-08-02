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

"""Streaming HTTP response implementation with async content delivery and trailer support."""

import typing
from functools import partial
from typing import Any

import anyio
from starlette._utils import collapse_excgroups  # type: ignore
from starlette.background import BackgroundTask
from starlette.concurrency import iterate_in_threadpool
from starlette.requests import ClientDisconnect
from starlette.responses import Response as Response
from starlette.types import Receive, Scope, Send

ContentStream = typing.Iterable[typing.Any] | typing.AsyncIterable[typing.Any]
AsyncContentStream = typing.AsyncIterable[typing.Any]


class StreamingResponse(Response):
    """A streaming HTTP response class that supports asynchronous content delivery with optional trailers.

    This class extends the base Response class to handle streaming content delivery,
    allowing for efficient transmission of large or dynamically generated content.
    It supports both synchronous and asynchronous iterables as content sources,
    HTTP trailers, and background tasks.

    Attributes:
        body_iterator (AsyncContentStream): An async iterator over the response body content.

    Features:
        - Automatic conversion of sync iterables to async using thread pools
        - HTTP trailer support with proper header advertisement
        - Client disconnect detection and handling
        - Background task execution after response completion
        - ASGI spec version compatibility (2.0+ with enhanced features for 2.4+)


    Note:
        For ASGI spec versions < 2.4, client disconnect detection is handled through
        concurrent task monitoring. For versions >= 2.4, native disconnect detection
        via OSError is used for better performance.
    """

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
        """Initialize a streaming response.

        Args:
            content: The content to stream, either an async iterable or content that will be iterated in a thread pool.
            status_code: HTTP status code for the response. Defaults to 200.
            headers: Optional mapping of HTTP headers to include in the response.
            trailers: Optional mapping of HTTP trailers to include in the response.
            media_type: Optional media type for the response. If None, uses the existing media_type.
            background: Optional background task to run after the response is sent.

        Returns:
            None
        """
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
        """ASGI application callable that handles HTTP response streaming with disconnect detection.

        This method implements the ASGI application interface, handling different ASGI spec versions
        and managing client disconnections during response streaming.

        Args:
            scope (Scope): ASGI scope dictionary containing request information and server capabilities
            receive (Receive): ASGI receive callable for receiving messages from the client
            send (Send): ASGI send callable for sending messages to the client

        Returns:
            None

        Raises:
            ClientDisconnect: When an OSError occurs during response streaming (client disconnected)

        Notes:
            - For ASGI spec version 2.4+: Uses simple streaming with OSError handling
            - For older versions: Uses task group with concurrent disconnect listening
            - Supports HTTP trailers when available in the server extensions
            - Executes background tasks after response completion if configured
        """
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
