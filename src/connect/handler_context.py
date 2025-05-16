import time


class HandlerContext:
    _deadline: float | None

    def __init__(self, timeout: float | None) -> None:
        self._deadline = time.time() + timeout if timeout else None

    def timeout_remaining(self) -> float | None:
        """Return the remaining time in seconds until the deadline, or None if no deadline is set.

        Returns:
            float | None: The number of seconds remaining until the deadline, or None if no deadline is set.

        """
        if self._deadline is None:
            return None

        return self._deadline - time.time()
