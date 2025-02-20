"""Middleware for handling HTTP requests."""

from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from connect.handler import Handler
from connect.utils import get_route_path, request_response

HandleFunc = Callable[[Request], Awaitable[Response]]


class ConnectMiddleware:
    """Middleware for handling ASGI applications with unary handlers.

    Attributes:
        app (ASGIApp): The ASGI application to wrap.
        handlers (list[Handler]): A list of unary handlers to process requests.

    """

    app: ASGIApp
    handlers: list[Handler]

    def __init__(self, app: ASGIApp, handlers: list[Handler]) -> None:
        """Initialize the middleware with the given ASGI application and handlers.

        Args:
            app (ASGIApp): The ASGI application instance.
            handlers (list[Handler]): A list of unary handlers to process requests.

        """
        self.app = app
        self.handlers = handlers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Asynchronous callable method to handle incoming ASGI requests.

        This method intercepts HTTP requests, determines the appropriate handler
        based on the route path, and delegates the request to the handler if found.
        If no handler is found, the request is passed to the next application in the
        middleware stack.

        Args:
            scope (Scope): The ASGI scope dictionary containing request information.
            receive (Receive): The ASGI receive callable to receive messages.
            send (Send): The ASGI send callable to send messages.

        Returns:
            None

        """
        if scope["type"] == "http":
            route_path = get_route_path(scope)
            handler = self._match_handler(route_path)
            if handler:
                app = request_response(handler.handle)
                await app(scope, receive, send)
                return

        await self.app(scope, receive, send)

    def _match_handler(self, route_path: str) -> Handler | None:
        if route_path == "":
            return None

        for handler in self.handlers:
            if route_path == handler.procedure:
                return handler

        return None
