"""Defines the IdempotencyLevel enumeration."""

from enum import IntEnum


class IdempotencyLevel(IntEnum):
    """Defines the idempotency level of an API operation.

    Idempotency is the property of certain operations that can be applied
    multiple times without changing the result beyond the initial application.
    In the context of APIs, this means that making the same request multiple
    times will have the same effect as making it once. This is crucial for
    building robust systems that can safely retry requests in case of
    network failures or other transient errors.

    Attributes:
        IDEMPOTENCY_UNKNOWN: The idempotency level is not specified or known.
            This is the default value.
        NO_SIDE_EFFECTS: The operation has no side effects on the server state.
            It is safe to retry indefinitely. This typically corresponds to
            read operations like HTTP GET.
        IDEMPOTENT: The operation is idempotent. It can be safely retried as
            multiple identical requests will produce the same result as a single
            request. This typically corresponds to operations like HTTP PUT or DELETE.
    """

    IDEMPOTENCY_UNKNOWN = 0

    NO_SIDE_EFFECTS = 1

    IDEMPOTENT = 2
