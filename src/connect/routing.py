"""Provides routing functionalities for a Connect-based application.

It includes classes and functions to handle unary RPC calls, convert request handlers
into ASGI applications, and manage routes within the application.
"""

import functools
from collections.abc import Awaitable, Callable
from typing import Any

from starlette import routing
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match
from starlette.types import ASGIApp, Receive, Scope, Send

from connect.handler import UnaryHandler
from connect.utils import is_async_callable, run_in_threadpool


def request_response(func: Callable[[Request], Awaitable[Response] | Response]) -> ASGIApp:
    """Convert a request handler function into an ASGI application.

    This decorator takes a function that handles a request and returns a response,
    and wraps it into an ASGI application callable. The handler function can be either
    synchronous or asynchronous.

    Args:
        func (Callable[[Request], Awaitable[Response] | Response]): The request handler function.
            It can be a synchronous function returning a Response or an asynchronous function
            returning an Awaitable of Response.

    Returns:
        ASGIApp: An ASGI application callable that can be used to handle ASGI requests.

    """
    f: Callable[[Request], Awaitable[Response]] = (
        func if is_async_callable(func) else functools.partial(run_in_threadpool, func)  # type:ignore
    )

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive, send)
        response = await f(request)
        await response(scope, receive, send)

    return app


class ConnectRoute(routing.BaseRoute):
    """A route class for handling unary RPC calls in a Connect-based application.

    Attributes:
        handler (UnaryHandler): The handler responsible for processing the RPC call.

    """

    handler: UnaryHandler

    def __init__(self, handler: UnaryHandler, *args: Any, **kwargs: Any) -> None:
        """Initialize the routing with a handler and optional arguments.

        Args:
            handler (UnaryHandler): The handler to be used for routing.
            *args (Any): Additional positional arguments.
            **kwargs (Any): Additional keyword arguments.

        """
        super().__init__(*args, **kwargs)
        self.handler = handler

    def matches(self, scope: Scope) -> tuple[Match, Scope]:
        """Determine if the given scope matches the route.

        Args:
            scope (Scope): The scope to be checked, which contains information about the request.

        Returns:
            tuple[Match, Scope]: A tuple where the first element is the match result (FULL or NONE),
                                and the second element is the original scope.

        """
        if scope["type"] == "http":
            route_path = get_route_path(scope)
            if route_path == self.handler.procedure:
                return Match.FULL, scope

        return Match.NONE, scope

    async def handle(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Asynchronous method to handle incoming ASGI requests.

        This method wraps the handler's `handle` method with `request_response`
        and then awaits the resulting ASGI application.

        Args:
            scope (Scope): The ASGI connection scope dictionary.
            receive (Receive): The ASGI receive callable.
            send (Send): The ASGI send callable.

        Returns:
            None

        """
        app = request_response(self.handler.handle)
        await app(scope, receive, send)


class ConnectRouter(routing.Router):
    """A router class that extends the functionality of the base routing.Router class.

    Attributes:
        routes (list[routing.BaseRoute]): A list of route objects that the router will handle.

    Methods:
        __init__(routes: list[routing.BaseRoute], *args: Any, **kwargs: Any) -> None:
            Initializes the ConnectRouter with a list of routes and any additional arguments.

        __call__(scope: Scope, receive: Receive, send: Send) -> None:
            Asynchronously handles incoming requests based on the scope type (http, websocket, lifespan).
            Updates the scope with the router instance and delegates request handling to the appropriate route.

    """

    routes: list[routing.BaseRoute]

    def __init__(self, routes: list[routing.BaseRoute], *args: Any, **kwargs: Any) -> None:
        """Initialize the routing with a list of routes.

        Args:
            routes (list[routing.BaseRoute]): A list of route objects.
            *args (Any): Variable length argument list.
            **kwargs (Any): Arbitrary keyword arguments.

        """
        super().__init__(*args, **kwargs)
        self.routes = routes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle incoming ASGI connections.

        This method is an entry point for ASGI applications. It processes the incoming
        connection based on the scope type (http, websocket, lifespan) and routes it
        accordingly.

        Args:
            scope (Scope): The connection scope containing details about the request.
            receive (Receive): An awaitable callable that receives events.
            send (Send): An awaitable callable that sends events.

        Returns:
            None

        """
        assert scope["type"] in ("http", "websocket", "lifespan")

        if "router" not in scope:
            scope["connect.router"] = self

        if scope["type"] == "lifespan":
            await self.lifespan(scope, receive, send)
            return

        for route in self.routes:
            match, child_scope = route.matches(scope)
            if match == routing.Match.FULL:
                scope.update(child_scope)
                await route.handle(scope, receive, send)
                return


def get_route_path(scope: Scope) -> str:
    """Extract the route path from the given scope.

    Args:
        scope (Scope): The scope dictionary containing the request information.

    Returns:
        str: The extracted route path. If a root path is specified in the scope,
            the function returns the path relative to the root path. If the path
            does not start with the root path or if the path is equal to the root
            path, the function returns the original path or an empty string,
            respectively.

    """
    path: str = scope["path"]
    root_path = scope.get("root_path", "")
    if not root_path:
        return path

    if not path.startswith(root_path):
        return path

    if path == root_path:
        return ""

    if path[len(root_path)] == "/":
        return path[len(root_path) :]

    return path
