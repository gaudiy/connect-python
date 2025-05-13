"""Provides utility functions for asynchronous programming."""

import asyncio
import contextlib
import functools
import typing
from collections.abc import (
    Iterator,
)

import anyio.to_thread
import httpcore

from connect.code import Code
from connect.error import ConnectError

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
    try:
        attr_value = getattr(obj, attr)
        if callable(attr_value):
            return attr_value
        return None
    except AttributeError:
        return None


def get_acallable_attribute(obj: object, attr: str) -> typing.Callable[..., typing.Awaitable[typing.Any]] | None:
    """Retrieve an attribute from an object if it is both callable and asynchronous.

    Args:
        obj (object): The object from which to retrieve the attribute.
        attr (str): The name of the attribute to retrieve.

    Returns:
        typing.Callable[..., typing.Awaitable[typing.Any]] | None:
            The attribute if it is callable and asynchronous, otherwise None.

    """
    try:
        attr_value = getattr(obj, attr)
        if callable(attr_value) and is_async_callable(attr_value):
            return attr_value
        return None
    except AttributeError:
        return None


async def aiterate[T](iterable: typing.Iterable[T]) -> typing.AsyncIterator[T]:
    """Turn a plain iterable into an async iterator.

    Args:
        iterable (typing.Iterable[T]): The iterable to convert.

    Yields:
        typing.AsyncIterator[T]: An async iterator over the elements of the input iterable.

    """
    for i in iterable:
        yield i


def _load_httpcore_exceptions() -> dict[type[Exception], Code]:
    return {
        httpcore.TimeoutException: Code.DEADLINE_EXCEEDED,
        httpcore.ConnectTimeout: Code.DEADLINE_EXCEEDED,
        httpcore.ReadTimeout: Code.DEADLINE_EXCEEDED,
        httpcore.WriteTimeout: Code.DEADLINE_EXCEEDED,
        httpcore.PoolTimeout: Code.RESOURCE_EXHAUSTED,
        httpcore.NetworkError: Code.UNAVAILABLE,
        httpcore.ConnectError: Code.UNAVAILABLE,
        httpcore.ReadError: Code.UNAVAILABLE,
        httpcore.WriteError: Code.UNAVAILABLE,
        httpcore.ProxyError: Code.UNAVAILABLE,
        httpcore.UnsupportedProtocol: Code.INVALID_ARGUMENT,
        httpcore.ProtocolError: Code.INVALID_ARGUMENT,
        httpcore.LocalProtocolError: Code.INTERNAL,
        httpcore.RemoteProtocolError: Code.INTERNAL,
    }


HTTPCORE_EXC_MAP: dict[type[Exception], Code] = {}


@contextlib.contextmanager
def map_httpcore_exceptions() -> Iterator[None]:
    """Map exceptions raised by the HTTP core to custom exceptions.

    This function uses a global exception map `HTTPCORE_EXC_MAP` to translate exceptions
    raised within its context. If the map is empty, it loads the exceptions using the
    `_load_httpcore_exceptions` function. When an exception is caught, it checks if the
    exception matches any in the map and raises a `ConnectError` with the corresponding
    error code. If no match is found, the original exception is re-raised.

    Yields:
        None: This function is a generator used as a context manager.

    Raises:
        ConnectError: If the caught exception matches an entry in `HTTPCORE_EXC_MAP`.
        Exception: If no match is found in `HTTPCORE_EXC_MAP`, the original exception is re-raised.

    """
    global HTTPCORE_EXC_MAP
    if len(HTTPCORE_EXC_MAP) == 0:
        HTTPCORE_EXC_MAP = _load_httpcore_exceptions()
    try:
        yield
    except Exception as exc:
        for from_exc, to_code in HTTPCORE_EXC_MAP.items():
            if isinstance(exc, from_exc):
                raise ConnectError(str(exc), to_code) from exc

        raise exc
