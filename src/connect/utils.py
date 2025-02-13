"""Provides utility functions for asynchronous programming."""

import asyncio
import functools
import typing
from collections.abc import Awaitable, Callable

import anyio.to_thread
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

type AwaitableCallable[T] = typing.Callable[..., typing.Awaitable[T]]


@typing.overload
def is_async_callable[T](obj: AwaitableCallable[T]) -> typing.TypeGuard[AwaitableCallable[T]]: ...


@typing.overload
def is_async_callable(obj: typing.Any) -> typing.TypeGuard[AwaitableCallable[typing.Any]]: ...


def is_async_callable(obj: typing.Any) -> typing.Any:
    """Check if the given object is an asynchronous callable.

    This function unwraps functools.partial objects to check if the underlying
    function is an asynchronous coroutine function. It returns True if the object
    is an async coroutine function or if it is a callable object whose __call__ method
    is an async coroutine function.

    Args:
        obj (typing.Any): The object to check.

    Returns:
        bool: True if the object is an asynchronous callable, False otherwise.

    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))


async def run_in_threadpool[T, **P](func: typing.Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Run a function in a thread pool and return the result.

    This function is useful for running synchronous code in an asynchronous context
    by offloading the execution to a thread pool.

    Args:
        func (typing.Callable[P, T]): The function to run in the thread pool.
        *args (P.args): Positional arguments to pass to the function.
        **kwargs (P.kwargs): Keyword arguments to pass to the function.

    Returns:
        T: The result of the function execution.

    Raises:
        Exception: Any exception raised by the function will be propagated.

    Example:
        result = await run_in_threadpool(some_sync_function, arg1, arg2, kwarg1=value1)

    """
    func = functools.partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(func)


def get_callable_attribute(obj: object, attr: str) -> typing.Callable[..., typing.Any] | None:
    """Retrieve a callable attribute from an object if it exists and is callable.

    Args:
        obj (object): The object from which to retrieve the attribute.
        attr (str): The name of the attribute to retrieve.

    Returns:
        typing.Callable[..., typing.Any] | None: The callable attribute if it exists and is callable, otherwise None.

    """
    if hasattr(obj, attr) and callable(getattr(obj, attr)):
        return getattr(obj, attr)

    return None


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

    async def async_func(request: Request) -> Response:
        if is_async_callable(func):
            return await func(request)
        else:
            return typing.cast(Response, await run_in_threadpool(func, request))

    f: Callable[[Request], Awaitable[Response]] = async_func

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive, send)
        response = await f(request)
        await response(scope, receive, send)

    return app


class AsyncByteStream(typing.AsyncIterable[bytes]):
    """An asynchronous byte stream for reading and writing byte chunks."""

    aiterator: typing.AsyncIterable[bytes] | None
    aclose_func: typing.Callable[..., typing.Awaitable[None]] | None

    def __init__(
        self,
        aiterator: typing.AsyncIterable[bytes] | None = None,
        aclose_func: typing.Callable[..., typing.Awaitable[None]] | None = None,
    ) -> None:
        """Initialize the asynchronous byte stream with the given iterator and close function."""
        self.aiterator = aiterator
        self.aclose_func = aclose_func

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        """Asynchronous iterator method to read byte chunks from the stream."""
        if self.aiterator is not None:
            async for chunk in self.aiterator:
                yield chunk

    async def aclose(self) -> None:
        """Asynchronously close the byte stream."""
        if self.aclose_func is not None:
            await self.aclose_func()


async def aiterate[T](iterable: typing.Iterable[T]) -> typing.AsyncIterator[T]:
    """Turn a plain iterable into an async iterator.

    Args:
        iterable (typing.Iterable[T]): The iterable to convert.

    Yields:
        typing.AsyncIterator[T]: An async iterator over the elements of the input iterable.

    """
    for i in iterable:
        yield i
