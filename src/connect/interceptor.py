"""Defines interceptors and request/response classes for unary and streaming RPC calls."""

import abc
from collections.abc import AsyncIterable, Awaitable, Callable
from typing import Any

from connect.request import ConnectRequest, Req
from connect.response import ConnectResponse, Res


class DescMessage:
    """Description of the message."""

    pass


class DescService:
    """Description of the service."""

    pass


class MessageShape:
    """Shape of the message."""

    pass


class DescMethodUnary:
    """Description of a unary method."""

    pass


class DescMethodStreaming:
    """Description of a streaming method."""

    pass


class ContextValues:
    """Values for the context."""

    pass


class UnaryRequest:
    """Unary request for a unary method."""

    def __init__(
        self,
        message: MessageShape,
        method: DescMethodUnary,
        service: DescService,
        url: str,
        signal: Any,
        header: dict[Any, Any],
        context_values: ContextValues,
    ):
        """Initialize the unary request."""
        self.stream = False
        self.message = message
        self.method = method
        self.service = service
        self.url = url
        self.signal = signal
        self.header = header
        self.context_values = context_values


class UnaryResponse:
    """Unary response for a unary method."""

    def __init__(
        self,
        message: MessageShape,
        method: DescMethodUnary,
        service: DescService,
        header: dict[Any, Any],
        trailer: dict[Any, Any],
    ):
        """Initialize the unary response."""
        self.stream = False
        self.message = message
        self.method = method
        self.service = service
        self.header = header
        self.trailer = trailer


class StreamRequest:
    """Stream request for a streaming method."""

    def __init__(
        self,
        message: AsyncIterable[MessageShape],
        method: DescMethodStreaming,
        service: DescService,
        url: str,
        signal: Any,
        header: dict[Any, Any],
        context_values: ContextValues,
    ):
        """Initialize the stream request."""
        self.stream = True
        self.message = message
        self.method = method
        self.service = service
        self.url = url
        self.signal = signal
        self.header = header
        self.context_values = context_values


class StreamResponse:
    """Stream response for a streaming method."""

    def __init__(
        self,
        message: AsyncIterable[MessageShape],
        method: DescMethodStreaming,
        service: DescService,
        header: dict[Any, Any],
        trailer: dict[Any, Any],
    ):
        """Initialize the stream response."""
        self.stream = True
        self.message = message
        self.method = method
        self.service = service
        self.header = header
        self.trailer = trailer


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
