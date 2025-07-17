# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Utility functions for async operations, HTTP exception handling, and callable attribute inspection."""

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
    """Check if an object is an async callable (coroutine function or callable with async __call__).

    This function handles partial functions by unwrapping them to check the underlying
    function. It returns True if the object is a coroutine function or if it's a
    callable object with an async __call__ method.

    Args:
        obj (typing.Any): The object to check for async callability.

    Returns:
        typing.Any: True if the object is async callable, False otherwise.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))


async def run_in_threadpool[T, **P](func: typing.Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Execute a synchronous function in a thread pool asynchronously.

    This function takes a synchronous callable and runs it in a separate thread
    using anyio's thread pool, allowing async code to call blocking functions
    without blocking the event loop.

    Args:
        func: A callable function to execute in the thread pool
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The return value of the executed function
    """
    func = functools.partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(func)


def get_callable_attribute(obj: object, attr: str) -> typing.Callable[..., typing.Any] | None:
    """Get a callable attribute from an object.

    This function attempts to retrieve an attribute from an object and returns it
    only if the attribute exists and is callable. If the attribute doesn't exist
    or is not callable, returns None.

    Args:
        obj (object): The object from which to retrieve the attribute.
        attr (str): The name of the attribute to retrieve.

    Returns:
        typing.Callable[..., typing.Any] | None: The callable attribute if it exists
            and is callable, otherwise None.
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
    """Convert a regular iterable to an async iterator.

    This function takes a synchronous iterable and yields each item asynchronously,
    allowing it to be used in async contexts with `async for` loops.

    Args:
        iterable: A synchronous iterable of type T.

    Yields:
        T: Each item from the input iterable, yielded asynchronously.
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
    """Context manager that maps httpcore exceptions to ConnectError exceptions.

    This function lazily loads a mapping of httpcore exceptions to Connect error codes
    and converts any httpcore exceptions that occur within its context to ConnectError
    instances with appropriate error codes.

    Yields:
        None: This is a context manager that yields control to the calling code.

    Raises:
        ConnectError: If an httpcore exception occurs that has a mapping defined,
                     it will be converted to a ConnectError with the appropriate code.
        Exception: Any other exceptions are re-raised unchanged.
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
