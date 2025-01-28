"""Defines interceptors and request/response classes for unary and streaming RPC calls."""

import abc
from collections.abc import Awaitable, Callable
from typing import Any

from connect.connect import ConnectRequest, ConnectResponse

UnaryFunc = Callable[[ConnectRequest[Any]], Awaitable[ConnectResponse[Any]]]


class Interceptor(abc.ABC):
    """Abstract base class for interceptors that can wrap unary functions."""

    @abc.abstractmethod
    def wrap_unary(self, next: UnaryFunc) -> UnaryFunc:
        """Wrap a unary function with the interceptor."""
        raise NotImplementedError()


def apply_interceptors(next: UnaryFunc, interceptors: list[Interceptor] | None) -> UnaryFunc:
    """Apply a list of interceptors to a unary function.

    Args:
        next (UnaryFunc): The original unary function to be wrapped by interceptors.
        interceptors (list[Interceptor]): A list of interceptors to apply. Each interceptor
                                          should have a method `wrap_unary` that takes a
                                          UnaryFunc and returns a wrapped UnaryFunc.

    Returns:
        UnaryFunc: The unary function wrapped with all the provided interceptors.

    """
    if interceptors is None:
        return next

    _next = next
    for interceptor in interceptors:
        _next = interceptor.wrap_unary(_next)

    return _next
