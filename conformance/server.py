import asyncio
import logging
import typing

import google.protobuf.any_pb2 as any_pb2
from gen.connectrpc.conformance.v1 import config_pb2, service_pb2
from gen.connectrpc.conformance.v1.conformancev1connect.service_connect import (
    ConformanceServiceHandler,
    create_ConformanceService_handlers,
)
from starlette.applications import Starlette
from starlette.middleware import Middleware

from connect.code import Code
from connect.connect import UnaryRequest, UnaryResponse
from connect.error import ConnectError, ErrorDetail
from connect.headers import Headers
from connect.middleware import ConnectMiddleware

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("conformance.server")


def headers_from_svc_headers(headers: typing.Iterable[service_pb2.Header]) -> Headers:
    """Convert a list of headers to a Headers object."""
    header = Headers()
    for h in headers:
        if key := header.get(h.name.lower()):
            header[key] = f"{header[key]}, {', '.join(h.value)}"
        else:
            header[h.name.lower()] = ", ".join(h.value)
    return header


def svc_headers_from_headers(headers: Headers) -> list[service_pb2.Header]:
    """Convert a Headers object to a list of headers."""
    svc_headers = []
    for key, value in headers.items():
        svc_headers.append(service_pb2.Header(name=key, value=[v.strip() for v in value.split(", ")]))

    return svc_headers


def svc_query_params_from_peer_query(query: typing.Mapping[str, str]) -> list[service_pb2.Header]:
    """Convert a query mapping to a list of headers."""
    svc_query_params = []
    for key, value in query.items():
        svc_query_params.append(service_pb2.Header(name=key, value=[v.strip() for v in value.split(", ")]))

    return svc_query_params


def code_from_svc_code(code: config_pb2.Code) -> Code:
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
    async def Unary(self, request: UnaryRequest[service_pb2.UnaryRequest]) -> UnaryResponse[service_pb2.UnaryResponse]:
        """Handle a unary request."""
        try:
            response_definition = request.message.response_definition

            request_any = any_pb2.Any()
            request_any.Pack(request.message)

            request_info = service_pb2.ConformancePayload.RequestInfo(
                request_headers=svc_headers_from_headers(request.headers),
                requests=[request_any],
                timeout_ms=None,
                connect_get_info=service_pb2.ConformancePayload.ConnectGetInfo(
                    query_params=svc_query_params_from_peer_query(request.peer.query),
                ),
            )

            error = None
            if response_definition.HasField("error"):
                detail = any_pb2.Any()
                detail.Pack(request_info)
                response_definition.error.details.append(detail)

                headers = headers_from_svc_headers(response_definition.response_headers)
                trailers = headers_from_svc_headers(response_definition.response_trailers)

                metadata = Headers()
                metadata.update(headers)
                metadata.update(trailers)

                error = ConnectError(
                    message=response_definition.error.message,
                    code=code_from_svc_code(response_definition.error.code),
                    details=[ErrorDetail(pb_any=error) for error in response_definition.error.details],
                    metadata=metadata,
                )
            else:
                payload = service_pb2.ConformancePayload(
                    data=response_definition.response_data,
                    request_info=request_info,
                )

                if response_definition:
                    headers = headers_from_svc_headers(response_definition.response_headers)
                    trailers = headers_from_svc_headers(response_definition.response_trailers)

            if response_definition.response_delay_ms:
                await asyncio.sleep(response_definition.response_delay_ms / 1000)

            if error:
                raise error

        except ConnectError:
            raise

        except Exception:
            raise

        return UnaryResponse(message=service_pb2.UnaryResponse(payload=payload), headers=headers, trailers=trailers)


middleware = [
    Middleware(
        ConnectMiddleware,
        create_ConformanceService_handlers(service=ConformanceService()),
    )
]

app = Starlette(middleware=middleware)
