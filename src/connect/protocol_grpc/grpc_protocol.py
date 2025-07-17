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

"""gRPC and gRPC-Web protocol implementation for Connect framework."""

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
    """ProtocolGRPC is a protocol implementation for handling gRPC and gRPC-Web communication.

    This class provides methods to create protocol handlers and clients that are configured
    for either standard gRPC or gRPC-Web, depending on the `web` flag provided at initialization.

    Attributes:
        web (bool): Indicates whether the protocol instance is configured for gRPC-Web.

    Methods:
        __init__(web: bool) -> None:
            Initializes the ProtocolGRPC instance, setting the mode to gRPC or gRPC-Web.

        handler(params: ProtocolHandlerParams) -> ProtocolHandler:
            Creates and returns a GRPCHandler instance with content types determined by the codecs
            and the protocol mode (gRPC or gRPC-Web).

        client(params: ProtocolClientParams) -> ProtocolClient:
            Creates and returns a GRPCClient instance, configuring the peer protocol and address
            based on the provided parameters and the protocol mode.
    """

    def __init__(self, web: bool) -> None:
        """Initializes the instance with the specified web mode.

        Args:
            web (bool): Indicates whether to use web mode.
        """
        self.web = web

    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Creates and returns a GRPCHandler instance configured with appropriate content types based on the provided parameters.

        Args:
            params (ProtocolHandlerParams): The parameters containing codec information and other handler configuration.

        Returns:
            ProtocolHandler: An instance of GRPCHandler initialized with the correct content types for gRPC or gRPC-Web.

        Behavior:
            - Determines the default and prefix content types based on whether gRPC-Web is enabled.
            - Constructs a list of supported content types from the available codecs.
            - Adds the bare content type if the 'proto' codec is present.
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
        """Creates and returns a ProtocolClient instance configured for gRPC or gRPC-Web communication.

        Args:
            params (ProtocolClientParams): Parameters required to configure the protocol client, including the target URL.

        Returns:
            ProtocolClient: An instance of GRPCClient initialized with the provided parameters and peer configuration.

        Notes:
            - If the instance is configured for web usage (`self.web` is True), the protocol is set to gRPC-Web.
            - The peer's address is constructed from the host and port in `params.url`, defaulting to an empty host and port 80 if not specified.
        """
        peer = Peer(
            address=Address(host=params.url.host or "", port=params.url.port or 80),
            protocol=PROTOCOL_GRPC,
            query={},
        )
        if self.web:
            peer.protocol = PROTOCOL_GRPC_WEB

        return GRPCClient(params, peer, self.web)
