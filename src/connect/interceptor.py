"""Defines interceptors and request/response classes for unary and streaming RPC calls."""

import abc
from collections.abc import Awaitable, Callable

from connect.request import ConnectRequest, Req
from connect.response import ConnectResponse, Res

UnaryFunc = Callable[[ConnectRequest[Req]], Awaitable[ConnectResponse[Res]]]


class Interceptor(abc.ABC):
    """Abstract base class for interceptors that can wrap unary functions."""

    @abc.abstractmethod
    def wrap_unary(self, next: UnaryFunc[Req, Res]) -> UnaryFunc[Req, Res]:
        """Wrap a unary function with the interceptor."""
        raise NotImplementedError()


def apply_interceptors(next: UnaryFunc[Req, Res], interceptors: list[Interceptor]) -> UnaryFunc[Req, Res]:
    """Apply a list of interceptors to a unary function.

    Args:
        next (UnaryFunc[Req, Res]): The unary function to be intercepted.
        interceptors (list[Interceptor]): A list of interceptors to apply. If None, the original function is returned.

    Returns:
        UnaryFunc[Req, Res]: The intercepted unary function.

    """
    if interceptors is None:
        return next

    _next = next
    for interceptor in interceptors:
        _next = interceptor.wrap_unary(_next)

    return _next
