"""Module provides the implementation for the ping service."""

import abc
from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Any

from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor

from connect.handler import UnaryHandler
from connect.options import ConnectOptions
from connect.request import ConnectRequest, Request
from connect.response import ConnectResponse, Response
from tests.testdata.ping.v1 import ping_pb2
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse


class PingServiceProcedures(Enum):
    """Procedures for the ping service."""

    Ping = "/tests.testdata.ping.v1.PingService/Ping"


PingService_service_descriptor: ServiceDescriptor = ping_pb2.DESCRIPTOR.services_by_name["PingService"]

PingService_Ping_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name["Ping"]


class PingServiceHandler(metaclass=abc.ABCMeta):
    """Handler for the ping service."""

    @abc.abstractmethod
    async def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]: ...


def add_PingService_to_handler(
    handler: PingServiceHandler, options: ConnectOptions | None = None
) -> Callable[..., Coroutine[Any, Any, Response]]:
    """Add the ping service to the handler."""
    pingServicePing_handler = UnaryHandler(
        PingServiceProcedures.Ping.value, handler.Ping, PingRequest, PingResponse, options
    )

    async def handle(request: Request) -> Response:
        path = request.url.path
        match path:
            case PingServiceProcedures.Ping.value:
                return await pingServicePing_handler.handle(request)
            case _:
                raise NotImplementedError(f"Path {path} not implemented")

    return handle


def create_PingService_handlers(service: PingServiceHandler) -> list[UnaryHandler]:
    rpc_handlers = [
        UnaryHandler(
            PingServiceProcedures.Ping.value,
            service.Ping,
            PingRequest,
            PingResponse,
        )
    ]
    return rpc_handlers
