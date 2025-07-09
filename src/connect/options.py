"""Defines configuration options for Connect RPC clients and handlers."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from connect.client_interceptor import ClientInterceptor
from connect.handler_interceptor import HandlerInterceptor
from connect.idempotency_level import IdempotencyLevel


class ConnectOptions(BaseModel):
    """Configuration options for a Connect RPC client or handler.

    This class encapsulates various settings that control the behavior of Connect
    protocol operations. It is used to configure interceptors, RPC-specific details,
    and data handling limits.

    Attributes:
        interceptors (list[HandlerInterceptor]): A list of interceptors to apply to the handler.
        descriptor (Any): The descriptor for the RPC method.
        idempotency_level (IdempotencyLevel): The idempotency level of the RPC method.
        require_connect_protocol_header (bool): A boolean indicating whether requests
            using the Connect protocol should include the header.
        compress_min_bytes (int): The minimum number of bytes to compress.
        read_max_bytes (int): The maximum number of bytes to read.
        send_max_bytes (int): The maximum number of bytes to send.
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

    def merge(self, override_options: "ConnectOptions | None" = None) -> "ConnectOptions":
        """Merges the current options with override options to create a new instance.

        This method combines the settings from the current `ConnectOptions` object
        with another `ConnectOptions` object provided as an override. The values
        from the `override_options` will take precedence over the values in the
        current object. Only the fields that were explicitly set in the
        `override_options` object are used for merging.

        If `override_options` is None, the method returns the current instance
        without any changes.

        Args:
            override_options (ConnectOptions | None, optional):
                The options object whose explicitly set values will override the
                current options. Defaults to None.

        Returns:
            ConnectOptions: A new `ConnectOptions` instance containing the merged
                options, or the original instance if `override_options` is None.
        """
        if override_options is None:
            return self

        merged_data = self.model_dump()
        explicit_overrides = override_options.model_dump(exclude_unset=True)
        merged_data.update(explicit_overrides)

        return ConnectOptions(**merged_data)


class ClientOptions(BaseModel):
    """Configuration options for a Connect client.

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
