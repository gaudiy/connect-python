"""Middleware for handling HTTP requests."""

from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from connect.handler import UnaryHandler
from connect.utils import get_route_path, request_response

HandleFunc = Callable[[Request], Awaitable[Response]]


class ConnectMiddleware:
    """Middleware for handling ASGI applications with unary handlers.

    Attributes:
        app (ASGIApp): The ASGI application to wrap.
        handlers (list[UnaryHandler]): A list of unary handlers to process requests.

    """

    app: ASGIApp
    handlers: list[UnaryHandler]

    def __init__(self, app: ASGIApp, handlers: list[UnaryHandler]) -> None:
        """Initialize the middleware with the given ASGI application and handlers.

        Args:
            app (ASGIApp): The ASGI application instance.
            handlers (list[UnaryHandler]): A list of unary handlers to process requests.

        """
        self.app = app
        self.handlers = handlers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Asynchronous callable method for the middleware.

        This method is invoked when the middleware instance is called. It processes
        HTTP requests by iterating through the registered handlers and invoking the
        appropriate handler based on the route path. If the request type is not HTTP,
        it directly forwards the request to the next application in the middleware chain.

        Args:
            scope (Scope): The ASGI scope dictionary containing request information.
            receive (Receive): The ASGI receive callable to receive messages.
            send (Send): The ASGI send callable to send messages.

        Returns:
            None

        """
        if scope["type"] == "http":
            rote_path = get_route_path(scope)
            for handler in self.handlers:
                if rote_path != handler.procedure:
                    await self.app(scope, receive, send)

                app = request_response(handler.handle)
                await app(scope, receive, send)
            pass
        else:
            await self.app(scope, receive, send)
