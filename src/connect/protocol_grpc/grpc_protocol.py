"""Protocol implementation for handling gRPC and gRPC-Web requests."""

from connect.codec import CodecNameType
from connect.connect import (
    Address,
    Peer,
)
from connect.protocol import (
    PROTOCOL_GRPC,
    PROTOCOL_GRPC_WEB,
    Protocol,
    ProtocolClient,
    ProtocolClientParams,
    ProtocolHandler,
    ProtocolHandlerParams,
)
from connect.protocol_grpc.constants import (
    GRPC_CONTENT_TYPE_DEFAULT,
    GRPC_CONTENT_TYPE_PREFIX,
    GRPC_WEB_CONTENT_TYPE_DEFAULT,
    GRPC_WEB_CONTENT_TYPE_PREFIX,
)
from connect.protocol_grpc.grpc_client import GRPCClient
from connect.protocol_grpc.grpc_handler import GRPCHandler


class ProtocolGRPC(Protocol):
    """ProtocolGRPC is a protocol implementation for handling gRPC and gRPC-Web requests.

    Attributes:
        web (bool): Indicates whether to use gRPC-Web (True) or standard gRPC (False).

    """

    def __init__(self, web: bool) -> None:
        """Initialize the instance.

        Args:
            web (bool): Indicates whether the instance is for web usage.

        """
        self.web = web

    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Create and returns a GRPCHandler instance configured with appropriate content types based on the provided parameters.

        Args:
            params (ProtocolHandlerParams): The parameters containing codec information and other handler configuration.

        Returns:
            ProtocolHandler: An instance of GRPCHandler initialized with the correct content types for gRPC or gRPC-Web.

        Behavior:
            - Determines the default and prefix content types based on whether gRPC-Web is enabled.
            - Constructs a list of supported content types from the available codecs.
            - Adds the bare content type if the PROTO codec is present.
            - Returns a GRPCHandler with the computed content types.

        """
        bare, prefix = GRPC_CONTENT_TYPE_DEFAULT, GRPC_CONTENT_TYPE_PREFIX
        if self.web:
            bare, prefix = GRPC_WEB_CONTENT_TYPE_DEFAULT, GRPC_WEB_CONTENT_TYPE_PREFIX

        content_types: list[str] = []
        for name in params.codecs.names():
            content_types.append(prefix + name)

        if params.codecs.get(CodecNameType.PROTO):
            content_types.append(bare)

        return GRPCHandler(params, self.web, content_types)

    def client(self, params: ProtocolClientParams) -> ProtocolClient:
        """Create and return a GRPCClient instance.

        Args:
            params (ProtocolClientParams): The parameters required to initialize the client.

        Returns:
            ProtocolClient: An instance of GRPCClient.

        """
        peer = Peer(
            address=Address(host=params.url.host or "", port=params.url.port or 80),
            protocol=PROTOCOL_GRPC,
            query={},
        )
        if self.web:
            peer.protocol = PROTOCOL_GRPC_WEB

        return GRPCClient(params, peer, self.web)
