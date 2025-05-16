"""Options and configuration for making calls, including timeout and abort event support."""

import asyncio

from pydantic import BaseModel, ConfigDict, Field


class CallOptions(BaseModel):
    """Options for configuring a call, such as timeout and abort event."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    timeout: float | None = Field(default=None)
    """Timeout for the call in seconds."""

    abort_event: asyncio.Event | None = Field(default=None)
    """Event to abort the call."""
