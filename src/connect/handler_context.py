"""Manages the context for a handler, particularly for handling timeouts."""

import time


class HandlerContext:
    """Manages the context for a handler, particularly for handling timeouts.

    This class allows setting a deadline upon initialization and provides a method
    to check the remaining time until that deadline.

    Attributes:
        _deadline (float | None): The timestamp for the deadline, or None if no timeout is set.
    """

    _deadline: float | None

    def __init__(self, timeout: float | None) -> None:
        """Initializes a new handler context.

        Args:
            timeout: The timeout in seconds. If None, no deadline is set.
        """
        self._deadline = time.time() + timeout if timeout else None

    def timeout_remaining(self) -> float | None:
        """Calculates the remaining time in seconds until the handler's deadline.

        If the request has no deadline, this method returns None. Otherwise, it
        returns the difference between the deadline and the current time.

        Returns:
            float | None: The remaining time in seconds, or None if no deadline is set.
        """
        if self._deadline is None:
            return None

        return self._deadline - time.time()
