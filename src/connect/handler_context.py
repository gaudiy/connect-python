class HandlerContext:
    timeout: float | None

    def __init__(self, timeout: float | None) -> None:
        self.timeout = timeout

    def timeout_remaining(self) -> float:
        if self.timeout is None:
            return 0

        return self.timeout
