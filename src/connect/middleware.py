"""Middleware for handling HTTP requests."""

from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

HandleFunc = Callable[[Request], Awaitable[Response]]


class ConnectMiddleware:
    """ConnectMiddleware is an ASGI middleware that processes incoming HTTP requests.

    Attributes:
        app (ASGIApp): The ASGI application instance.
        handle (HandleFunc): A function to handle the request and return a response.

    """

    app: ASGIApp
    handle: HandleFunc

    def __init__(self, app: ASGIApp, handle: HandleFunc) -> None:
        """Initialize the middleware with an ASGI application."""
        self.app = app
        self.handle = handle

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Asynchronous callable method to handle incoming ASGI requests.

        Args:
            scope (Scope): The scope of the request, containing details such as type, path, headers, etc.
            receive (Receive): An awaitable callable that will yield events as they are received.
            send (Send): An awaitable callable that will be used to send events back to the client.

        Returns:
            None

        """
        if scope["type"] == "http":
            request = Request(scope, receive)
            response = await self.handle(request)
            await response(scope, receive, send)
        else:
            await self.app(scope, receive, send)
