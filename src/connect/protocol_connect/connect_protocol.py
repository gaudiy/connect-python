"""Provides the ProtocolConnect class for handling connection protocols."""

from http import HTTPMethod

from connect.connect import (
    StreamType,
)
from connect.idempotency_level import IdempotencyLevel
from connect.protocol import (
    Protocol,
    ProtocolClient,
    ProtocolClientParams,
    ProtocolHandlerParams,
)
from connect.protocol_connect.connect_client import ConnectClient
from connect.protocol_connect.connect_handler import ConnectHandler
from connect.protocol_connect.constants import (
    CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
    CONNECT_UNARY_CONTENT_TYPE_PREFIX,
)


class ProtocolConnect(Protocol):
    """ProtocolConnect is a class that implements the Protocol interface for handling connection protocols."""

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        """Handle the creation of a ConnectHandler based on the provided ProtocolHandlerParams.

        Args:
            params (ProtocolHandlerParams): The parameters required to create the ConnectHandler.

        Returns:
            ConnectHandler: An instance of ConnectHandler configured with the appropriate methods and content types.

        """
        methods = [HTTPMethod.POST]

        if params.spec.stream_type == StreamType.Unary and params.idempotency_level == IdempotencyLevel.NO_SIDE_EFFECTS:
            methods.append(HTTPMethod.GET)

        content_types: list[str] = []
        for name in params.codecs.names():
            if params.spec.stream_type == StreamType.Unary:
                content_types.append(CONNECT_UNARY_CONTENT_TYPE_PREFIX + name)
                continue

            content_types.append(CONNECT_STREAMING_CONTENT_TYPE_PREFIX + name)

        return ConnectHandler(params, methods=methods, accept=content_types)

    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Create and returns a ConnectClient instance.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the client.

        Returns:
            ProtocolClient: An instance of ConnectClient.

        """
        return ConnectClient(params)
