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

"""Defines handler-side interceptors for the Connect RPC framework."""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeGuard, overload

from connect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from connect.handler_context import HandlerContext

UnaryFunc = Callable[[UnaryRequest[Any], HandlerContext], Awaitable[UnaryResponse[Any]]]
StreamFunc = Callable[[StreamRequest[Any], HandlerContext], Awaitable[StreamResponse[Any]]]


class HandlerInterceptor:
    """A handler-side interceptor for wrapping and modifying RPC handlers.

    Interceptors are a powerful mechanism for observing and modifying RPCs without
    changing the core application logic. They can be used for tasks like logging,
    authentication, metrics collection, or adding custom headers.

    A HandlerInterceptor instance is configured with wrapper functions that are
    applied to the RPC handlers. The Connect framework will call these wrappers
    before invoking the actual handler.

    Attributes:
        wrap_unary (Callable[[UnaryFunc], UnaryFunc] | None): A callable that
            takes a unary RPC handler and returns a new, wrapped handler. The
            framework calls this function to build the handler chain.
        wrap_stream (Callable[[StreamFunc], StreamFunc] | None): A callable that
            takes a streaming RPC handler and returns a new, wrapped handler. The
            framework calls this function to build the handler chain.
    """

    wrap_unary: Callable[[UnaryFunc], UnaryFunc] | None = None
    wrap_stream: Callable[[StreamFunc], StreamFunc] | None = None


def is_unary_func(next: UnaryFunc | StreamFunc) -> TypeGuard[UnaryFunc]:
    """Type guard to determine if a handler function is a unary function.

    This function inspects the signature of the provided callable (`next`) to
    determine if it matches the expected signature of a `UnaryFunc`. A function
    is considered a unary function if it is callable, accepts exactly two
    parameters, and the type annotation of its first parameter is `UnaryRequest`.

    Args:
        next: The handler function to be checked, which can be either a
              `UnaryFunc` or a `StreamFunc`.

    Returns:
        True if the function signature corresponds to a `UnaryFunc`,
        False otherwise.
    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 2
        and getattr(parameters[0].annotation, "__origin__", None) is UnaryRequest
    )


def is_stream_func(next: UnaryFunc | StreamFunc) -> TypeGuard[StreamFunc]:
    """Type guard to determine if a handler function is a StreamFunc.

    A function is considered a StreamFunc if it is a callable that accepts
    two arguments, and the first argument is annotated as a StreamRequest.

    Args:
        next: The function to inspect.

    Returns:
        True if the function signature matches StreamFunc, False otherwise.
    """
    signature = inspect.signature(next)
    parameters = list(signature.parameters.values())
    return bool(
        callable(next)
        and len(parameters) == 2
        and getattr(parameters[0].annotation, "__origin__", None) is StreamRequest
    )


@overload
def apply_interceptors(next: UnaryFunc, interceptors: list[HandlerInterceptor] | None) -> UnaryFunc: ...


@overload
def apply_interceptors(next: StreamFunc, interceptors: list[HandlerInterceptor] | None) -> StreamFunc: ...


def apply_interceptors(
    next: UnaryFunc | StreamFunc, interceptors: list[HandlerInterceptor] | None
) -> UnaryFunc | StreamFunc:
    """Applies a list of interceptors to a handler function.

    This function takes a handler function (either unary or streaming) and wraps it
    with the provided interceptors. The interceptors are applied in the order they
    appear in the list, meaning the first interceptor in the list will be the
    outermost wrapper and the last to execute before the actual handler.

    Args:
        next: The handler function (either UnaryFunc or StreamFunc) to be wrapped.
        interceptors: An optional list of HandlerInterceptor instances. If None,
            the original handler function is returned unmodified.

    Returns:
        The wrapped handler function, which is of the same type as the input `next`
        function.

    Raises:
        ValueError: If the `next` function is not a valid UnaryFunc or StreamFunc.
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
