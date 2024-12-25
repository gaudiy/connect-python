"""Middleware for handling HTTP requests."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

T = TypeVar("T")
Kwargs = Mapping[str, Any]
HandleFunc = Callable[[Request], Awaitable[tuple[bytes, Mapping[str, str]]]]


class ConnectMiddleware:
    """Middleware for handling HTTP requests."""

    app: ASGIApp
    handle: HandleFunc

    def __init__(self, app: ASGIApp, handle: HandleFunc) -> None:
        """Initialize the middleware with an ASGI application."""
        self.app = app
        self.handle = handle

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an ASGI scope."""
        if scope["type"] == "http":
            request = Request(scope, receive)
            res_bytes, headers = await self.handle(request)
            response = Response(content=res_bytes, headers=headers, status_code=200)
            await response(scope, receive, send)
        else:
            await self.app(scope, receive, send)
