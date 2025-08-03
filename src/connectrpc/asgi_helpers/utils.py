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

"""Utility functions for ASGI helpers, including request/response decorators and route path extraction."""

import typing
from collections.abc import (
    Awaitable,
    Callable,
)

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from connectrpc.utils import is_async_callable, run_in_threadpool


def request_response(func: Callable[[Request], Awaitable[Response] | Response]) -> ASGIApp:
    """Convert a request handler function into an ASGI application.

    This decorator takes a function that handles a request and returns a response,
    and wraps it into an ASGI application callable. The handler function can be either
    synchronous or asynchronous.

    Args:
        func (Callable[[Request], Awaitable[Response] | Response]): The request handler function.
            It can be a synchronous function returning a Response or an asynchronous function
            returning an Awaitable of Response.

    Returns:
        ASGIApp: An ASGI application callable that can be used to handle ASGI requests.

    """

    async def async_func(request: Request) -> Response:
        if is_async_callable(func):
            return await func(request)
        else:
            return typing.cast(Response, await run_in_threadpool(func, request))

    f: Callable[[Request], Awaitable[Response]] = async_func

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive, send)
        response = await f(request)
        await response(scope, receive, send)

    return app


def get_route_path(scope: Scope) -> str:
    """Extract the route path from the given scope.

    Args:
        scope (Scope): The scope dictionary containing the request information.

    Returns:
        str: The extracted route path. If a root path is specified in the scope,
            the function returns the path relative to the root path. If the path
            does not start with the root path or if the path is equal to the root
            path, the function returns the original path or an empty string,
            respectively.

    """
    path: str = scope["path"]
    root_path = scope.get("root_path", "")
    if not root_path:
        return path

    if not path.startswith(root_path):
        return path

    if path == root_path:
        return ""

    if path[len(root_path)] == "/":
        return path[len(root_path) :]

    return path
