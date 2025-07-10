"""Module provides the implementation for the ping service."""

import abc
from enum import Enum

from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor

from connect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from connect.handler import ClientStreamHandler, Handler, ServerStreamHandler, UnaryHandler
from connect.handler_context import HandlerContext
from connect.options import HandlerOptions
from tests.testdata.ping.v1 import ping_pb2
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse


class PingServiceProcedures(Enum):
    """Procedures for the ping service."""

    Ping = "/tests.testdata.ping.v1.PingService/Ping"
    PingServerStream = "/tests.testdata.ping.v1.PingService/PingServerStream"
    PingClientStream = "/tests.testdata.ping.v1.PingService/PingClientStream"


PingService_service_descriptor: ServiceDescriptor = ping_pb2.DESCRIPTOR.services_by_name["PingService"]

PingService_Ping_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name["Ping"]
PingService_PingServerStream_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name[
    "PingServerStream"
]
PingService_PingClientStream_method_descriptor: MethodDescriptor = PingService_service_descriptor.methods_by_name[
    "PingClientStream"
]


class PingServiceHandler(metaclass=abc.ABCMeta):
    """Handler for the ping service."""

    async def Ping(self, request: UnaryRequest[PingRequest], context: HandlerContext) -> UnaryResponse[PingResponse]: ...

    async def PingServerStream(self, request: StreamRequest[PingRequest], context: HandlerContext) -> StreamResponse[PingResponse]: ...

    async def PingClientStream(self, request: StreamRequest[PingRequest], context: HandlerContext) -> StreamResponse[PingResponse]: ...


def create_PingService_handlers(service: PingServiceHandler, options: HandlerOptions | None = None) -> list[Handler]:
    handlers: list[Handler] = [
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
        ClientStreamHandler(
            procedure=PingServiceProcedures.PingClientStream.value,
            stream=service.PingClientStream,
            input=PingRequest,
            output=PingResponse,
            options=options,
        ),
    ]
    return handlers
