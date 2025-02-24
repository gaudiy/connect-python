"""Module provides the implementation for the ping service."""

import abc
from enum import Enum

from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor

from connect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from connect.handler import Handler, ServerStreamHandler, UnaryHandler
from connect.options import ConnectOptions
from tests.testdata.ping.v1 import ping_pb2
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse


class PingServiceProcedures(Enum):
    """Procedures for the ping service."""

    Ping = "/tests.testdata.ping.v1.PingService/Ping"
    PingServerStream = "/tests.testdata.ping.v1.PingService/PingServerStream"


PingService_service_descriptor: ServiceDescriptor = ping_pb2.DESCRIPTOR.services_by_name["PingService"]

PingService_Ping_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name["Ping"]
PingService_PingServerStream_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name[
    "PingServerStream"
]


class PingServiceHandler(metaclass=abc.ABCMeta):
    """Handler for the ping service."""

    async def Ping(self, request: UnaryRequest[PingRequest]) -> UnaryResponse[PingResponse]: ...

    async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]: ...


def create_PingService_handlers(service: PingServiceHandler, options: ConnectOptions | None = None) -> list[Handler]:
    rpc_handlers: list[Handler] = [
        UnaryHandler(
            procedure=PingServiceProcedures.Ping.value,
            unary=service.Ping,
            input=PingRequest,
            output=PingResponse,
            options=options,
        ),
        ServerStreamHandler(
            procedure=PingServiceProcedures.PingServerStream.value,
            stream=service.PingServerStream,
            input=PingRequest,
            output=PingResponse,
            options=options,
        ),
    ]
    return rpc_handlers
