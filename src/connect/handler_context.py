"""Provides the HandlerContext class for managing operation timeouts and tracking remaining time."""

import time


class HandlerContext:
    """HandlerContext manages an optional timeout for operations, allowing tracking of the remaining time until a deadline.

    Attributes:
        _deadline (float | None): The UNIX timestamp representing the deadline, or None if no timeout is set.

    """

    _deadline: float | None

    def __init__(self, timeout: float | None) -> None:
        """Initialize HandlerContext with an optional timeout.

        Args:
            timeout (float | None): The timeout duration in seconds, or None for no timeout.

        """
        self._deadline = time.time() + timeout if timeout else None

    def timeout_remaining(self) -> float | None:
        """Return the remaining time in seconds until the deadline, or None if no deadline is set.

        Returns:
            float | None: The number of seconds remaining until the deadline, or None if no deadline is set.

        """
        if self._deadline is None:
            return None

        return self._deadline - time.time()
