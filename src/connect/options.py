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

"""Defines configuration options for Connect RPC clients and handlers."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from connect.client_interceptor import ClientInterceptor
from connect.handler_interceptor import HandlerInterceptor
from connect.idempotency_level import IdempotencyLevel


class HandlerOptions(BaseModel):
    """Configuration options for a handler.

    This class encapsulates various settings that control the behavior of a handler
    in the Connect protocol implementation. It allows for customization of interceptors,
    protocol requirements, and data handling limits.

    Attributes:
        interceptors (list[HandlerInterceptor]): A list of interceptors to apply to the handler.
        descriptor (Any): The descriptor for the RPC method.
        idempotency_level (IdempotencyLevel): The idempotency level of the RPC method.
        require_connect_protocol_header (bool): A boolean indicating whether requests
            using the Connect protocol should include the protocol version header.
        compress_min_bytes (int): The minimum number of bytes for a response to be
            eligible for compression. A value of -1 disables compression.
        read_max_bytes (int): The maximum number of bytes to read from a request body.
            A value of -1 indicates no limit.
        send_max_bytes (int): The maximum number of bytes to send in a response body.
            A value of -1 indicates no limit.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    interceptors: list[HandlerInterceptor] = Field(default=[])
    """A list of interceptors to apply to the handler."""

    descriptor: Any = Field(default="")
    """The descriptor for the RPC method."""

    idempotency_level: IdempotencyLevel = Field(default=IdempotencyLevel.IDEMPOTENCY_UNKNOWN)
    """The idempotency level of the RPC method."""

    require_connect_protocol_header: bool = Field(default=False)
    """A boolean indicating whether requests using the Connect protocol should include the header."""

    compress_min_bytes: int = Field(default=-1)
    """The minimum number of bytes to compress."""

    read_max_bytes: int = Field(default=-1)
    """The maximum number of bytes to read."""

    send_max_bytes: int = Field(default=-1)
    """The maximum number of bytes to send."""

    def merge(self, override_options: "HandlerOptions | None" = None) -> "HandlerOptions":
        """Merges this HandlerOptions instance with another, creating a new instance.

        The values from the `override_options` will take precedence over the
        values in the current instance. Only the fields that are explicitly set
        in `override_options` are used for the merge.

        Args:
            override_options: An optional HandlerOptions object to merge with.

        Returns:
            A new HandlerOptions instance with the merged options. If
            `override_options` is None, the original instance is returned.
        """
        if override_options is None:
            return self

        merged_data = self.model_dump()
        explicit_overrides = override_options.model_dump(exclude_unset=True)
        merged_data.update(explicit_overrides)

        return HandlerOptions(**merged_data)


class ClientOptions(BaseModel):
    """Configuration options for a client.

    This class holds settings that control the behavior of client-side RPC calls,
    such as interceptors, compression, and protocol-specific details.

    Attributes:
        interceptors (list[ClientInterceptor]): A list of interceptors to apply to the handler.
        descriptor (Any): The descriptor for the RPC method.
        idempotency_level (IdempotencyLevel): The idempotency level of the RPC method.
        request_compression_name (str | None): The name of the compression method to use for requests.
        compress_min_bytes (int): The minimum number of bytes to compress.
        read_max_bytes (int): The maximum number of bytes to read.
        send_max_bytes (int): The maximum number of bytes to send.
        enable_get (bool): A boolean indicating whether to enable GET requests.
        protocol (Literal["connect", "grpc", "grpc-web"]): The protocol to use for the request.
        use_binary_format (bool): A boolean indicating whether to use binary format for the request.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    interceptors: list[ClientInterceptor] = Field(default=[])
    """A list of interceptors to apply to the handler."""

    descriptor: Any = Field(default="")
    """The descriptor for the RPC method."""

    idempotency_level: IdempotencyLevel = Field(default=IdempotencyLevel.IDEMPOTENCY_UNKNOWN)
    """The idempotency level of the RPC method."""

    request_compression_name: str | None = Field(default=None)
    """The name of the compression method to use for requests."""

    compress_min_bytes: int = Field(default=-1)
    """The minimum number of bytes to compress."""

    read_max_bytes: int = Field(default=-1)
    """The maximum number of bytes to read."""

    send_max_bytes: int = Field(default=-1)
    """The maximum number of bytes to send."""

    enable_get: bool = Field(default=False)
    """A boolean indicating whether to enable GET requests."""

    protocol: Literal["connect", "grpc", "grpc-web"] = Field(default="connect")
    """The protocol to use for the request."""

    use_binary_format: bool = Field(default=True)
    """A boolean indicating whether to use binary format for the request."""

    def merge(self, override_options: "ClientOptions | None" = None) -> "ClientOptions":
        """Creates a new ClientOptions instance by merging with override options.

        If override_options is provided, this method returns a new ClientOptions
        instance that is a copy of the current options, updated with any
        explicitly set values from the override_options. If override_options
        is None, it returns the current instance.

        Args:
            override_options (ClientOptions | None, optional):
                The options to merge with. Fields explicitly set in this
                object will override the corresponding values in the current
                options. Defaults to None.

        Returns:
            ClientOptions: A new instance with the merged options, or the
                current instance if no override options are provided.
        """
        if override_options is None:
            return self

        merged_data = self.model_dump()
        explicit_overrides = override_options.model_dump(exclude_unset=True)
        merged_data.update(explicit_overrides)

        return ClientOptions(**merged_data)
