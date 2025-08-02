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

"""Module implementing the ConformanceService for testing connect conformance."""

import asyncio
import logging
import typing

import google.protobuf.any_pb2 as any_pb2
from gconnect.code import Code
from gconnect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from gconnect.error import ConnectError, ErrorDetail
from gconnect.handler_context import HandlerContext
from gconnect.headers import Headers
from gconnect.middleware import ConnectMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware

from gen.connectrpc.conformance.v1 import config_pb2, service_pb2
from gen.connectrpc.conformance.v1.conformancev1connect.service_connect import (
    ConformanceServiceHandler,
    create_ConformanceService_handlers,
)

logger = logging.getLogger("conformance.server")


def headers_from_pb_headers(headers: typing.Iterable[service_pb2.Header]) -> Headers:
    """Convert a list of headers to a Headers object."""
    header = Headers()
    for h in headers:
        if key := header.get(h.name.lower()):
            header[key] = f"{header[key]}, {', '.join(h.value)}"
        else:
            header[h.name.lower()] = ", ".join(h.value)
    return header


def pb_headers_from_headers(headers: Headers) -> list[service_pb2.Header]:
    """Convert a Headers object to a list of headers."""
    svc_headers = []
    for key, value in headers.items():
        svc_headers.append(service_pb2.Header(name=key, value=[v.strip() for v in value.split(", ")]))

    return svc_headers


def pb_query_params_from_peer_query(query: typing.Mapping[str, str]) -> list[service_pb2.Header]:
    """Convert a query mapping to a list of headers."""
    svc_query_params = []
    for key, value in query.items():
        svc_query_params.append(service_pb2.Header(name=key, value=[v.strip() for v in value.split(", ")]))

    return svc_query_params


def code_from_pb_code(code: config_pb2.Code) -> Code:
    """Convert a service code to a Connect code."""
    match code:
        case config_pb2.CODE_UNSPECIFIED:
            return Code.UNKNOWN
        case config_pb2.CODE_CANCELED:
            return Code.CANCELED
        case config_pb2.CODE_UNKNOWN:
            return Code.UNKNOWN
        case config_pb2.CODE_INVALID_ARGUMENT:
            return Code.INVALID_ARGUMENT
        case config_pb2.CODE_DEADLINE_EXCEEDED:
            return Code.DEADLINE_EXCEEDED
        case config_pb2.CODE_NOT_FOUND:
            return Code.NOT_FOUND
        case config_pb2.CODE_ALREADY_EXISTS:
            return Code.ALREADY_EXISTS
        case config_pb2.CODE_PERMISSION_DENIED:
            return Code.PERMISSION_DENIED
        case config_pb2.CODE_RESOURCE_EXHAUSTED:
            return Code.RESOURCE_EXHAUSTED
        case config_pb2.CODE_FAILED_PRECONDITION:
            return Code.FAILED_PRECONDITION
        case config_pb2.CODE_ABORTED:
            return Code.ABORTED
        case config_pb2.CODE_OUT_OF_RANGE:
            return Code.OUT_OF_RANGE
        case config_pb2.CODE_UNIMPLEMENTED:
            return Code.UNIMPLEMENTED
        case config_pb2.CODE_INTERNAL:
            return Code.INTERNAL
        case config_pb2.CODE_UNAVAILABLE:
            return Code.UNAVAILABLE
        case config_pb2.CODE_DATA_LOSS:
            return Code.DATA_LOSS
        case config_pb2.CODE_UNAUTHENTICATED:
            return Code.UNAUTHENTICATED
        case _:
            raise ValueError(f"Unsupported code: {code}")


class ConformanceService(ConformanceServiceHandler):
    """ConformanceService is a service handler that implements various gRPC methods for testing conformance."""

    async def Unary(
        self, request: UnaryRequest[service_pb2.UnaryRequest], context: HandlerContext
    ) -> UnaryResponse[service_pb2.UnaryResponse]:
        """Handle a unary gRPC request and generates a response based on the provided request definition.

        Args:
            request (UnaryRequest[service_pb2.UnaryRequest]): The incoming unary request containing
                the message and associated metadata.

        Returns:
            UnaryResponse[service_pb2.UnaryResponse]: The response containing the payload, headers,
                and trailers.

        Raises:
            ConnectError: If an error is defined in the response definition, it raises a ConnectError
                with the specified details, code, and metadata.
            Exception: If any other exception occurs during processing, it is raised.

        Behavior:
            - Extracts the response definition from the request message.
            - Packs the request message into an `Any` protobuf message.
            - Constructs a `RequestInfo` object containing headers, query parameters, and other metadata.
            - If the response definition specifies an error, it constructs a `ConnectError` with the
              provided details, headers, and trailers.
            - If no error is specified, it constructs a `ConformancePayload` with the response data
              and request information.
            - Applies a response delay if specified in the response definition.
            - Returns the constructed response or raises the error if defined.

        """
        response_definition = request.message.response_definition

        request_any = any_pb2.Any()
        request_any.Pack(request.message)

        timeout_sec = context.timeout_remaining()

        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=pb_headers_from_headers(request.headers),
            requests=[request_any],
            timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
            connect_get_info=service_pb2.ConformancePayload.ConnectGetInfo(
                query_params=pb_query_params_from_peer_query(request.peer.query),
            ),
        )

        error = None
        if response_definition.HasField("error"):
            detail = any_pb2.Any()
            detail.Pack(request_info)
            response_definition.error.details.append(detail)

            headers = headers_from_pb_headers(response_definition.response_headers)
            trailers = headers_from_pb_headers(response_definition.response_trailers)

            metadata = Headers()
            metadata.update(headers)
            metadata.update(trailers)

            error = ConnectError(
                message=response_definition.error.message,
                code=code_from_pb_code(response_definition.error.code),
                details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                metadata=metadata,
            )
        else:
            payload = service_pb2.ConformancePayload(
                data=response_definition.response_data,
                request_info=request_info,
            )

            if response_definition:
                headers = headers_from_pb_headers(response_definition.response_headers)
                trailers = headers_from_pb_headers(response_definition.response_trailers)

        if response_definition.response_delay_ms:
            await asyncio.sleep(response_definition.response_delay_ms / 1000)

        if error:
            raise error

        return UnaryResponse(content=service_pb2.UnaryResponse(payload=payload), headers=headers, trailers=trailers)

    async def IdempotentUnary(
        self, request: UnaryRequest[service_pb2.IdempotentUnaryRequest], context: HandlerContext
    ) -> UnaryResponse[service_pb2.IdempotentUnaryResponse]:
        """Handle the IdempotentUnary RPC call.

        This method processes a unary request and generates a unary response. It supports
        idempotent operations and handles various response definitions, including errors,
        response delays, and metadata headers/trailers.

        Args:
            request (UnaryRequest[service_pb2.IdempotentUnaryRequest]): The incoming unary
                request containing the message and metadata.

        Returns:
            UnaryResponse[service_pb2.IdempotentUnaryResponse]: The response containing the
                message, headers, and trailers.

        Raises:
            ConnectError: If an error is defined in the response definition or occurs during
                processing.
            Exception: For any other unexpected errors.

        """
        response_definition = request.message.response_definition

        request_any = any_pb2.Any()
        request_any.Pack(request.message)

        timeout_sec = context.timeout_remaining()
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=pb_headers_from_headers(request.headers),
            requests=[request_any],
            timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
            connect_get_info=service_pb2.ConformancePayload.ConnectGetInfo(
                query_params=pb_query_params_from_peer_query(request.peer.query),
            ),
        )

        error = None
        if response_definition.HasField("error"):
            detail = any_pb2.Any()
            detail.Pack(request_info)
            response_definition.error.details.append(detail)

            headers = headers_from_pb_headers(response_definition.response_headers)
            trailers = headers_from_pb_headers(response_definition.response_trailers)

            metadata = Headers()
            metadata.update(headers)
            metadata.update(trailers)

            error = ConnectError(
                message=response_definition.error.message,
                code=code_from_pb_code(response_definition.error.code),
                details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                metadata=metadata,
            )
        else:
            payload = service_pb2.ConformancePayload(
                data=response_definition.response_data,
                request_info=request_info,
            )

            if response_definition:
                headers = headers_from_pb_headers(response_definition.response_headers)
                trailers = headers_from_pb_headers(response_definition.response_trailers)

        if response_definition.response_delay_ms:
            await asyncio.sleep(response_definition.response_delay_ms / 1000)

        if error:
            raise error

        return UnaryResponse(
            content=service_pb2.IdempotentUnaryResponse(payload=payload), headers=headers, trailers=trailers
        )

    async def ClientStream(
        self, request: StreamRequest[service_pb2.ClientStreamRequest], context: HandlerContext
    ) -> StreamResponse[service_pb2.ClientStreamResponse]:
        """Handle a bidirectional streaming RPC where the client sends a stream of `ClientStreamRequest` messages and receives a single `ClientStreamResponse` message.

        Args:
            request (StreamRequest[service_pb2.ClientStreamRequest]):
                The incoming stream of `ClientStreamRequest` messages from the client.

        Returns:
            StreamResponse[service_pb2.ClientStreamResponse]:
                A response containing the processed payload, headers, and trailers.

        Raises:
            ConnectError: If an error is defined in the response definition or occurs during processing.
            Exception: For any other unexpected errors.

        Behavior:
            - Processes incoming messages from the client stream.
            - Extracts and packs messages into a list of `Any` protobuf objects.
            - Constructs a `ConformancePayload.RequestInfo` object with request details.
            - Handles response definitions, including errors, headers, trailers, and delays.
            - Raises a `ConnectError` if an error is specified in the response definition.
            - Returns a `StreamResponse` containing the payload, headers, and trailers.

        """
        response_definition = None
        messages = []

        async for message in request.messages:
            if response_definition is None:
                response_definition = message.response_definition

            message_any = any_pb2.Any()
            message_any.Pack(message)
            messages.append(message_any)

        timeout_sec = context.timeout_remaining()
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=pb_headers_from_headers(request.headers),
            requests=messages,
            timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
            connect_get_info=service_pb2.ConformancePayload.ConnectGetInfo(
                query_params=pb_query_params_from_peer_query(request.peer.query),
            ),
        )

        error = None
        payload = None
        if response_definition and response_definition.HasField("error"):
            detail = any_pb2.Any()
            detail.Pack(request_info)
            response_definition.error.details.append(detail)

            headers = headers_from_pb_headers(response_definition.response_headers)
            trailers = headers_from_pb_headers(response_definition.response_trailers)

            metadata = Headers()
            metadata.update(headers)
            metadata.update(trailers)

            error = ConnectError(
                message=response_definition.error.message,
                code=code_from_pb_code(response_definition.error.code),
                details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                metadata=metadata,
            )
        else:
            payload = service_pb2.ConformancePayload(
                request_info=request_info,
            )

            if response_definition:
                payload.data = response_definition.response_data
                headers = headers_from_pb_headers(response_definition.response_headers)
                trailers = headers_from_pb_headers(response_definition.response_trailers)

            if response_definition and response_definition.response_delay_ms:
                await asyncio.sleep(response_definition.response_delay_ms / 1000)

        if error:
            raise error

        return StreamResponse(
            content=service_pb2.ClientStreamResponse(payload=payload),
            headers=headers,
            trailers=trailers,
        )

    async def ServerStream(
        self, request: StreamRequest[service_pb2.ServerStreamRequest], context: HandlerContext
    ) -> StreamResponse[service_pb2.ServerStreamResponse]:
        """Handle a server-side streaming RPC call.

        This method processes a stream of incoming messages from the client,
        constructs a response based on the provided response definition, and
        streams the responses back to the client. It also supports sending
        headers, trailers, and handling errors.

        Args:
            request (StreamRequest[service_pb2.ServerStreamRequest]):
                The incoming stream request containing messages from the client.

        Returns:
            StreamResponse[service_pb2.ServerStreamResponse]:
                A stream response containing the outgoing messages, headers,
                and trailers.

        Raises:
            ConnectError: If an error occurs during the processing of the stream.
            Exception: For any other unexpected errors.

        """
        response_definition = None
        messages = []

        async for message in request.messages:
            if response_definition is None:
                response_definition = message.response_definition

            message_any = any_pb2.Any()
            message_any.Pack(message)
            messages.append(message_any)

        headers = None
        trailers = None
        if response_definition:
            headers = headers_from_pb_headers(response_definition.response_headers)
            trailers = headers_from_pb_headers(response_definition.response_trailers)

        timeout_sec = context.timeout_remaining()
        request_info = service_pb2.ConformancePayload.RequestInfo(
            request_headers=pb_headers_from_headers(request.headers),
            requests=messages,
            timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
            connect_get_info=service_pb2.ConformancePayload.ConnectGetInfo(
                query_params=pb_query_params_from_peer_query(request.peer.query),
            ),
        )

        async def iterator() -> typing.AsyncIterator[service_pb2.ServerStreamResponse]:
            first_response = True

            if response_definition is None:
                return

            for response_data in response_definition.response_data:
                if first_response:
                    payload = service_pb2.ConformancePayload(
                        data=response_data,
                        request_info=request_info,
                    )
                    first_response = False
                else:
                    payload = service_pb2.ConformancePayload(
                        data=response_data,
                    )

                if response_definition.response_delay_ms:
                    await asyncio.sleep(response_definition.response_delay_ms / 1000)

                yield service_pb2.ServerStreamResponse(
                    payload=payload,
                )

            if response_definition.HasField("error"):
                headers = headers_from_pb_headers(response_definition.response_headers)
                trailers = headers_from_pb_headers(response_definition.response_trailers)

                metadata = Headers()
                metadata.update(headers)
                metadata.update(trailers)

                if first_response:
                    detail = any_pb2.Any()
                    detail.Pack(request_info)
                    response_definition.error.details.append(detail)

                error = ConnectError(
                    message=response_definition.error.message,
                    code=code_from_pb_code(response_definition.error.code),
                    details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                    metadata=metadata,
                )

                raise error

        return StreamResponse(
            content=iterator(),
            headers=headers,
            trailers=trailers,
        )

    async def BidiStream(
        self, request: StreamRequest[service_pb2.BidiStreamRequest], context: HandlerContext
    ) -> StreamResponse[service_pb2.BidiStreamResponse]:
        """Handle a bidirectional streaming RPC.

        This method processes incoming messages from the client, constructs responses
        based on a predefined response definition, and streams the responses back to the client.

        Args:
            request (StreamRequest[service_pb2.BidiStreamRequest]): The incoming stream request
                containing client messages.

        Returns:
            StreamResponse[service_pb2.BidiStreamResponse]: The response stream containing
                server messages, along with optional headers and trailers.

        Raises:
            ConnectError: If an error is defined in the response definition or if an error
                occurs during processing.
            Exception: For any unexpected errors during processing.

        Notes:
            - The method processes incoming messages asynchronously.
            - If a response definition is provided in the first message, it is used to
              construct the responses, including headers, trailers, and potential delays.
            - If an error is defined in the response definition, it is raised as a
              `ConnectError` with the appropriate metadata and details.

        """
        response_definition = None
        messages = []
        first_response = True
        response_index = 0

        async for message in request.messages:
            message_any = any_pb2.Any()
            message_any.Pack(message)
            messages.append(message_any)

            if first_response:
                response_definition = message.response_definition
                first_response = False

                if response_definition:
                    headers = headers_from_pb_headers(response_definition.response_headers)
                    trailers = headers_from_pb_headers(response_definition.response_trailers)

        timeout_sec = context.timeout_remaining()

        async def iterator() -> typing.AsyncIterator[service_pb2.BidiStreamResponse]:
            nonlocal response_index

            while response_definition and response_index < len(response_definition.response_data):
                if response_index == 0:
                    request_info = service_pb2.ConformancePayload.RequestInfo(
                        request_headers=pb_headers_from_headers(request.headers),
                        requests=messages,
                        timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
                    )
                else:
                    request_info = None

                response = service_pb2.BidiStreamResponse(
                    payload=service_pb2.ConformancePayload(
                        request_info=request_info,
                        data=response_definition.response_data[response_index],
                    )
                )
                if response_definition.response_delay_ms:
                    await asyncio.sleep(response_definition.response_delay_ms / 1000)

                response_index += 1
                yield response

            if response_definition and response_definition.HasField("error"):
                headers = headers_from_pb_headers(response_definition.response_headers)
                trailers = headers_from_pb_headers(response_definition.response_trailers)

                metadata = Headers()
                metadata.update(headers)
                metadata.update(trailers)

                if response_index == 0:
                    request_info = service_pb2.ConformancePayload.RequestInfo(
                        request_headers=pb_headers_from_headers(request.headers),
                        requests=messages,
                        timeout_ms=int(timeout_sec * 1000) if timeout_sec else None,
                    )

                    detail = any_pb2.Any()
                    detail.Pack(request_info)
                    response_definition.error.details.append(detail)

                error = ConnectError(
                    message=response_definition.error.message,
                    code=code_from_pb_code(response_definition.error.code),
                    details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                    metadata=metadata,
                )

                raise error

        return StreamResponse(
            content=iterator(),
            headers=headers,
            trailers=trailers,
        )


middleware = [
    Middleware(
        ConnectMiddleware,
        create_ConformanceService_handlers(service=ConformanceService()),
    )
]

app = Starlette(middleware=middleware)
