"""Options for the UniversalHandler class."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from connect.idempotency_level import IdempotencyLevel
from connect.interceptor import Interceptor


class ConnectOptions(BaseModel):
    """Options for the connect command."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    interceptors: list[Interceptor] = Field(default=[])
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
