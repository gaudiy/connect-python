"""Define the ConnectASGI class, a subclass of Starlette.

This class initializes an ASGI application with custom handlers and routes.
"""

from typing import Any

from starlette import routing
from starlette.applications import Starlette
from starlette.routing import BaseRoute
from starlette.types import Receive, Scope, Send

from connect.handler import UnaryHandler
from connect.options import ConnectOptions
from connect.routing import ConnectRoute, ConnectRouter


class ConnectASGI(Starlette):
    """ConnectASGI is a subclass of Starlette that initializes an ASGI application with custom handlers and routes.

    Attributes:
        handlers (list[UnaryHandler]): A list of unary handlers for the application.
        options (ConnectOptions | None): Optional configuration options for the application.
        router (ConnectRouter): The router instance that manages the application's routes.

    """

    handlers: list[UnaryHandler]
    options: ConnectOptions | None = None
    router: ConnectRouter

    def __init__(self, handlers: list[UnaryHandler], *args: Any, **kwargs: Any) -> None:
        """Initialize the application with the given parameters.

        Args:
            handlers (list[UnaryHandler]): A list of unary handlers to be used by the application.
            *args (Any): Additional positional arguments.
            **kwargs (Any): Additional keyword arguments.

        Keyword Args:
            debug (bool): If True, enables debug mode. Defaults to False.
            routes (list[BaseRoute]): A list of routes to be added to the application.
            middleware (list): A list of middleware to be used by the application.
            exception_handlers (dict): A dictionary of exception handlers.
            on_startup (list[callable]): A list of functions to be called on startup.
            on_shutdown (list[callable]): A list of functions to be called on shutdown.
            lifespan (Any): Lifespan context manager.

        Returns:
            None

        """
        super().__init__(
            debug=bool(kwargs.get("debug", False)),
            routes=[route for route in kwargs.get("routes", []) if isinstance(route, BaseRoute)],
            middleware=list(kwargs.get("middleware", [])) if isinstance(kwargs.get("middleware", []), list) else [],
            exception_handlers=kwargs.get("exception_handlers"),
            on_startup=[func for func in kwargs.get("on_startup", []) if callable(func)],
            on_shutdown=[func for func in kwargs.get("on_shutdown", []) if callable(func)],
            lifespan=kwargs.get("lifespan"),
        )

        self.handlers = handlers
        self.router = ConnectRouter(self._get_routes_from_handlers(handlers), *args, **kwargs)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Asynchronous callable method to handle ASGI application scope, receive, and send.

        This method is invoked when the ASGI server calls the application instance.
        It sets the "connect.app" key in the scope dictionary to the current instance
        and then delegates the request handling to the router.

        Args:
            scope (Scope): The ASGI connection scope dictionary.
            receive (Receive): An awaitable callable that receives ASGI messages.
            send (Send): An awaitable callable that sends ASGI messages.

        Returns:
            None

        """
        scope["connect.app"] = self
        app = self.router

        # TODO(tsubakiky): Implement interceptors
        await app(scope, receive, send)

    def _get_routes_from_handlers(self, handlers: list[UnaryHandler]) -> list[routing.BaseRoute]:
        routes: list[routing.BaseRoute] = []
        for handler in handlers:
            routes.append(ConnectRoute(handler))

        return routes
