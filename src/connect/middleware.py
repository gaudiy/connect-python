"""Middleware for handling HTTP requests."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from connect.response import ConnectResponse

T = TypeVar("T")
Kwargs = Mapping[str, Any]
HandleFunc = Callable[[str, Kwargs], Awaitable[ConnectResponse[Any]]]


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
            path = scope["path"]
            request = await Request(scope, receive).body()
            connect_response = await self.handle(
                path,
                {
                    "body": request,
                    "headers": dict(scope["headers"]),
                    "query": dict(scope["query_string"]),
                },
            )
            response = connect_response.to_response()
            await response(scope, receive, send)
        else:
            await self.app(scope, receive, send)
