"""Defines interceptors and request/response classes for unary and streaming RPC calls."""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeGuard, overload

from connect.call_options import CallOptions
from connect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse

UnaryFunc = Callable[[UnaryRequest[Any], CallOptions], Awaitable[UnaryResponse[Any]]]
StreamFunc = Callable[[StreamRequest[Any], CallOptions], Awaitable[StreamResponse[Any]]]


class Interceptor:
    """Abstract base class for interceptors that can wrap unary functions."""

    wrap_unary: Callable[[UnaryFunc], UnaryFunc] | None = None
    wrap_stream: Callable[[StreamFunc], StreamFunc] | None = None


def is_unary_func(next: UnaryFunc | StreamFunc) -> TypeGuard[UnaryFunc]:
    """Determine if the given function is a unary function.

    A unary function is defined as a callable that takes a single parameter
    whose type annotation has an origin of `UnaryRequest`.

    Args:
        next (UnaryFunc | StreamFunc): The function to be checked.

    Returns:
        TypeGuard[UnaryFunc]: True if the function is a unary function, False otherwise.

    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 1
        and getattr(parameters[0].annotation, "__origin__", None) is UnaryRequest
    )


def is_stream_func(next: UnaryFunc | StreamFunc) -> TypeGuard[StreamFunc]:
    """Determine if the given function is a StreamFunc.

    This function checks if the provided function `next` is callable, has exactly one parameter,
    and if the annotation of that parameter has an origin of `StreamRequest`.

    Args:
        next (UnaryFunc | StreamFunc): The function to be checked.

    Returns:
        TypeGuard[StreamFunc]: True if `next` is a StreamFunc, False otherwise.

    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 1
        and getattr(parameters[0].annotation, "__origin__", None) is StreamRequest
    )


@overload
def apply_interceptors(next: UnaryFunc, interceptors: list[Interceptor] | None) -> UnaryFunc: ...


@overload
def apply_interceptors(next: StreamFunc, interceptors: list[Interceptor] | None) -> StreamFunc: ...


def apply_interceptors(next: UnaryFunc | StreamFunc, interceptors: list[Interceptor] | None) -> UnaryFunc | StreamFunc:
    """Apply a list of interceptors to a given function.

    Args:
        next (UnaryFunc | StreamFunc): The function to which interceptors will be applied.
                                    It can be either a unary function or a stream function.
        interceptors (list[Interceptor] | None): A list of interceptors to apply. If None, the original function is returned.

    Returns:
        UnaryFunc | StreamFunc: The function wrapped with the provided interceptors.

    Raises:
        ValueError: If an interceptor does not implement the required wrap method for the function type,
                or if the provided function type is invalid.

    """
    if interceptors is None:
        return next

    _next = next
    if is_unary_func(_next):
        for interceptor in interceptors:
            if interceptor.wrap_unary is None:
                break
            _next = interceptor.wrap_unary(_next)
        return _next

    elif is_stream_func(_next):
        for interceptor in interceptors:
            if interceptor.wrap_stream is None:
                break
            _next = interceptor.wrap_stream(_next)
        return _next
    else:
        raise ValueError(f"Invalid function type: {next}")
