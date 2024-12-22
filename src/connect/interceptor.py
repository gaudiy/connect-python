"""Defines interceptors and request/response classes for unary and streaming RPC calls."""

from collections.abc import AsyncIterable, Callable
from typing import Any


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


# AnyFn is a function that takes a UnaryRequest or StreamRequest and returns a UnaryResponse or StreamResponse.
AnyFn = Callable[[UnaryRequest | StreamRequest], UnaryResponse | StreamResponse]

# Interceptor is a function that takes a function and returns a function.
Interceptor = Callable[[AnyFn], AnyFn]


def apply_interceptors(next: AnyFn, interceptors: list[Interceptor] | None = None) -> AnyFn:
    """Apply a list of interceptors to a given function."""
    if interceptors:
        for interceptor in reversed(interceptors):
            next = interceptor(next)
    return next
