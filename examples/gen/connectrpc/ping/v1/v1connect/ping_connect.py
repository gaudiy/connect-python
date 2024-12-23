"""Module provides the implementation for the ping service."""

import abc
from collections.abc import Callable
from enum import Enum
from typing import Any

from gaudiy.ping.v1 import ping_pb2
from gen.connectrpc.ping.v1.ping_pb2 import PingRequest, PingResponse
from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor

from connect.hander import UnaryHander
from connect.options import ConnectOptions
from connect.request import ConnectRequest
from connect.response import ConnectResponse


class PingServiceProcedures(Enum):
    """Procedures for the ping service."""

    Ping = "/gaudiy.ping.v1.PingService/Ping"


PingService_service_descriptor: ServiceDescriptor = ping_pb2.DESCRIPTOR.services_by_name["PingService"]

PingService_Ping_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name["Ping"]


class PingServiceHandler(metaclass=abc.ABCMeta):  # noqa: UP004
    """Handler for the ping service."""

    @abc.abstractmethod
    def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]: ...


def add_PingService_to_handler(
    handler: PingServiceHandler, options: ConnectOptions | None = None
) -> Callable[..., None]:
    """Add the ping service to the handler."""
    pingServicePing_handler = UnaryHander(PingServiceProcedures.Ping.value, handler.Ping, options)

    def handle(path: str, **kwargs: Any) -> None:
        match path:
            case PingServiceProcedures.Ping.value:
                pingServicePing_handler.serve(**kwargs)

    return handle
