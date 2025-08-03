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

"""Client-side interceptor utilities for modifying RPC behavior in Connect Python."""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeGuard, overload

from connectrpc.call_options import CallOptions
from connectrpc.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse

UnaryFunc = Callable[[UnaryRequest[Any], CallOptions], Awaitable[UnaryResponse[Any]]]
StreamFunc = Callable[[StreamRequest[Any], CallOptions], Awaitable[StreamResponse[Any]]]


class ClientInterceptor:
    """A client-side interceptor for modifying RPC behavior.

    Interceptors are a powerful mechanism for implementing cross-cutting concerns
    like logging, metrics, authentication, and retries. They can inspect and
    modify requests and responses for both unary and streaming RPCs.

    To create an interceptor, you can instantiate this class directly, providing
    wrapper functions for `wrap_unary` and/or `wrap_stream`. These wrappers are
    higher-order functions that take an RPC handler and return a new, wrapped
    handler. The wrapped handler can then execute logic before and/or after
    invoking the original RPC.

    Attributes:
        wrap_unary (Callable[[UnaryFunc], UnaryFunc] | None): A function that
            takes a unary RPC handler and returns a new unary RPC handler.
            The returned handler is responsible for invoking the original
            handler. This is used to intercept unary (request-response) RPCs.
        wrap_stream (Callable[[StreamFunc], StreamFunc] | None): A function
            that takes a streaming RPC handler and returns a new streaming RPC
            handler. The returned handler is responsible for invoking the
            original handler. This is used to intercept streaming RPCs.
    """

    wrap_unary: Callable[[UnaryFunc], UnaryFunc] | None = None
    wrap_stream: Callable[[StreamFunc], StreamFunc] | None = None


def is_unary_func(next: UnaryFunc | StreamFunc) -> TypeGuard[UnaryFunc]:
    """Type guard to determine if a function is a UnaryFunc.

    This function inspects the signature of the provided callable `next`
    to determine if it matches the expected signature of a unary RPC handler.
    A function is considered a `UnaryFunc` if it is callable, accepts exactly
    two parameters, and its first parameter is type-hinted as a `UnaryRequest`.

    Args:
        next: The function to inspect, which can be either a UnaryFunc or a StreamFunc.

    Returns:
        True if the function signature matches `UnaryFunc`, False otherwise.
        This allows type checkers to narrow the type of `next` to `UnaryFunc`.
    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 2
        and getattr(parameters[0].annotation, "__origin__", None) is UnaryRequest
    )


def is_stream_func(next: UnaryFunc | StreamFunc) -> TypeGuard[StreamFunc]:
    """Determines if the given function is a stream function.

    A stream function is identified by being callable, having exactly two parameters,
    and the first parameter's annotation having an `__origin__` attribute equal to `StreamRequest`.

    Args:
        next (UnaryFunc | StreamFunc): The function to check.

    Returns:
        TypeGuard[StreamFunc]: True if the function is a stream function, False otherwise.
    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 2
        and getattr(parameters[0].annotation, "__origin__", None) is StreamRequest
    )


@overload
def apply_interceptors(next: UnaryFunc, interceptors: list[ClientInterceptor] | None) -> UnaryFunc: ...


@overload
def apply_interceptors(next: StreamFunc, interceptors: list[ClientInterceptor] | None) -> StreamFunc: ...


def apply_interceptors(
    next: UnaryFunc | StreamFunc, interceptors: list[ClientInterceptor] | None
) -> UnaryFunc | StreamFunc:
    """Applies a list of client interceptors to a unary or stream function.

    This function wraps the provided `next` function (either unary or stream) with the corresponding
    interceptor wrappers from the `interceptors` list. If an interceptor does not provide a wrapper
    for the function type, the wrapping process stops at that interceptor.

    Args:
        next (UnaryFunc | StreamFunc): The function to be wrapped by interceptors. Can be either a unary or stream function.
        interceptors (list[ClientInterceptor] | None): A list of client interceptors to apply. If None, the original function is returned.

    Returns:
        UnaryFunc | StreamFunc: The wrapped function with all applicable interceptors applied.

    Raises:
        ValueError: If the provided function is neither a unary nor a stream function.
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
