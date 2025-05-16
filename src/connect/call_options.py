import asyncio

from pydantic import BaseModel, ConfigDict, Field


class CallOptions(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timeout: float | None = Field(default=None)
    """Timeout for the call in seconds."""

    abort_event: asyncio.Event | None = Field(default=None)
    """Event to abort the call."""
