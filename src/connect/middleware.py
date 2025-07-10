"""Provides ASGI middleware for routing requests to Connect protocol handlers."""

from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from connect.asgi_helpers.utils import get_route_path, request_response
from connect.handler import Handler

HandleFunc = Callable[[Request], Awaitable[Response]]


class ConnectMiddleware:
    """ASGI middleware for routing requests to Connect-style handlers.

    This middleware intercepts incoming HTTP requests and attempts to match them
    against a list of registered `Handler` instances based on the request path.
    If a matching handler is found for the request's route, it processes the
    request and sends a response. If no handler matches the route, the request
    is forwarded to the next ASGI application in the stack.

    This allows for integrating Connect-protocol services within a standard
    ASGI application framework, such as Starlette or FastAPI.

    Attributes:
        app (ASGIApp): The next ASGI application in the middleware stack.
        handlers (list[Handler]): A list of Connect handlers to which requests
            can be routed.
    """

    app: ASGIApp
    handlers: list[Handler]

    def __init__(self, app: ASGIApp, handlers: list[Handler]) -> None:
        """Initializes the middleware.

        Args:
            app: The ASGI application.
            handlers: A list of handlers to be used by the middleware.
        """
        self.app = app
        self.handlers = handlers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """The ASGI application entry point.

        This method is called for each request. It checks if the request is an HTTP
        request and if the path matches any of the registered handlers. If a match
        is found, the request is handled by the corresponding handler. Otherwise,
        the request is passed on to the next ASGI application in the stack.

        Args:
            scope (Scope): The ASGI connection scope.
            receive (Receive): The ASGI receive channel.
            send (Send): The ASGI send channel.
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
