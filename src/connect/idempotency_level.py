"""Defines the IdempotencyLevel enumeration."""

from enum import IntEnum


class IdempotencyLevel(IntEnum):
    """IdempotencyLevel is an enumeration that represents different levels of idempotency.

    Attributes:
        IDEMPOTENCY_UNKNOWN (int): Represents an unknown idempotency level.
        NO_SIDE_EFFECTS (int): Indicates that the operation has no side effects.
        IDEMPOTENT (int): Indicates that the operation is idempotent, meaning it can be performed multiple times without changing the result.

    """

    IDEMPOTENCY_UNKNOWN = 0

    NO_SIDE_EFFECTS = 1

    IDEMPOTENT = 2
