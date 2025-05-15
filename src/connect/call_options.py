import asyncio

from pydantic import BaseModel, Field


class CallOptions(BaseModel):
    timeout: float | None = Field(default=None)
    """Timeout for the call in seconds."""

    abort_event: asyncio.Event | None = Field(default=None)
    """Event to abort the call."""
