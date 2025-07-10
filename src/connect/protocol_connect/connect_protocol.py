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
    """ProtocolConnect is a protocol handler for the Connect protocol, responsible for creating handler and client instances based on provided parameters.

    Methods:
        handler(params: ProtocolHandlerParams) -> ConnectHandler:
            Handles the creation of a ConnectHandler instance, configuring supported HTTP methods and accepted content types
            based on the stream type and idempotency level specified in the parameters.

        client(params: ProtocolClientParams) -> ProtocolClient:
            Creates and returns a ConnectClient instance initialized with the provided parameters.
    """

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        """Creates and returns a ConnectHandler instance configured with appropriate HTTP methods and accepted content types based on the provided ProtocolHandlerParams.

        Args:
            params (ProtocolHandlerParams): The parameters specifying the protocol handler configuration, including stream type, idempotency level, and codecs.

        Returns:
            ConnectHandler: An instance of ConnectHandler configured with the determined HTTP methods and accepted content types.

        Behavior:
            - Allows POST requests by default.
            - Adds GET as an allowed method if the stream type is Unary and the idempotency level is NO_SIDE_EFFECTS.
            - Constructs a list of accepted content types based on the stream type and available codec names.
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
        """Creates and returns a new instance of `ConnectClient` using the provided parameters.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the protocol client.

        Returns:
            ProtocolClient: An instance of `ConnectClient` initialized with the given parameters.
        """
        return ConnectClient(params)
