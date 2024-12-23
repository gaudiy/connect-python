"""Module provides the implementation for the ping service."""

import abc
from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Any

import google.protobuf.message
from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor

from connect.hander import UnaryHander
from connect.options import ConnectOptions
from connect.request import ConnectRequest
from connect.response import ConnectResponse
from tests.testdata.ping.v1 import ping_pb2
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse


class PingServiceProcedures(Enum):
    """Procedures for the ping service."""

    Ping = "/gaudiy.ping.v1.PingService/Ping"


PingService_service_descriptor: ServiceDescriptor = ping_pb2.DESCRIPTOR.services_by_name["PingService"]

PingService_Ping_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name["Ping"]


class PingServiceHandler(metaclass=abc.ABCMeta):
    """Handler for the ping service."""

    @abc.abstractmethod
    async def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]: ...


def add_PingService_to_handler(
    handler: PingServiceHandler, options: ConnectOptions | None = None
) -> Callable[..., Coroutine[Any, Any, ConnectResponse[google.protobuf.message.Message]]]:
    """Add the ping service to the handler."""
    pingServicePing_handler = UnaryHander(PingServiceProcedures.Ping.value, handler.Ping, options)

    async def handle(path: str, request: dict[Any, Any], **kwargs: Any) -> ConnectResponse[Any]:
        match path:
            case PingServiceProcedures.Ping.value:
                return await pingServicePing_handler.serve(request, **kwargs)
            case _:
                return ConnectResponse(None)

    return handle
