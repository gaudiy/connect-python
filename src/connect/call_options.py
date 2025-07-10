"""Call options configuration models."""

import asyncio

from pydantic import BaseModel, ConfigDict, Field


class CallOptions(BaseModel):
    """Options for configuring a call.

    Attributes:
        timeout (float | None): Timeout for the call in seconds. If None, no timeout is applied.
        abort_event (asyncio.Event | None): Event to abort the call. If set, the call can be cancelled by setting this event.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    timeout: float | None = Field(default=None)
    """Timeout for the call in seconds."""

    abort_event: asyncio.Event | None = Field(default=None)
    """Event to abort the call."""
