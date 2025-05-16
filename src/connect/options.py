"""Options for the UniversalHandler class."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from connect.client_interceptor import ClientInterceptor
from connect.handler_interceptor import HandlerInterceptor
from connect.idempotency_level import IdempotencyLevel


class ConnectOptions(BaseModel):
    """Options for the connect command."""

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
        """Merge this options object with an override options object.

        Args:
            override_options (ConnectOptions | None): Optional override options object.
                If None, this options object is returned as is.

        Returns:
            ConnectOptions: A new instance with attributes merged from both objects.

        """
        if override_options is None:
            return self

        merged_data = self.model_dump()
        explicit_overrides = override_options.model_dump(exclude_unset=True)
        merged_data.update(explicit_overrides)

        return ConnectOptions(**merged_data)


class ClientOptions(BaseModel):
    """Options for the Connect client."""

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
        """Merge this options object with an override options object.

        Args:
            override_options (ClientOptions | None): Optional override options object.
                If None, this options object is returned as is.

        Returns:
            ClientOptions: A new instance with attributes merged from both objects.

        """
        if override_options is None:
            return self

        merged_data = self.model_dump()
        explicit_overrides = override_options.model_dump(exclude_unset=True)
        merged_data.update(explicit_overrides)

        return ClientOptions(**merged_data)
