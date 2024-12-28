"""Provides utility functions for asynchronous programming."""

import asyncio
import functools
import typing

import anyio.to_thread

T = typing.TypeVar("T")
AwaitableCallable = typing.Callable[..., typing.Awaitable[T]]


@typing.overload
def is_async_callable(obj: AwaitableCallable[T]) -> typing.TypeGuard[AwaitableCallable[T]]: ...


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


P = typing.ParamSpec("P")
T = typing.TypeVar("T")


async def run_in_threadpool(func: typing.Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
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
