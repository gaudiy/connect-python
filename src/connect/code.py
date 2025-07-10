# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Connect represents categories of errors as codes."""

import enum


@enum.unique
class Code(enum.IntEnum):
    """Enum class representing various RPC error codes.

    Attributes:
      CANCELED (int): RPC canceled, usually by the caller.
      UNKNOWN (int): Unknown error.
      INVALID_ARGUMENT (int): Client specified an invalid argument.
      DEADLINE_EXCEEDED (int): Deadline expired before operation could complete.
      NOT_FOUND (int): Some requested entity was not found.
      ALREADY_EXISTS (int): Some entity that we attempted to create already exists.
      PERMISSION_DENIED (int): The caller does not have permission to execute the specified operation.
      RESOURCE_EXHAUSTED (int): Some resource has been exhausted.
      FAILED_PRECONDITION (int): Operation was rejected because the system is not in a state required for the operation's execution.
      ABORTED (int): The operation was aborted.
      OUT_OF_RANGE (int): Operation was attempted past the valid range.
      UNIMPLEMENTED (int): Operation is not implemented or not supported/enabled.
      INTERNAL (int): Internal errors.
      UNAVAILABLE (int): The service is currently unavailable.
      DATA_LOSS (int): Unrecoverable data loss or corruption.
      UNAUTHENTICATED (int): The request does not have valid authentication credentials for the operation.
    """

    CANCELED = 1
    """Canceled, usually be the user"""
    UNKNOWN = 2
    """Unknown error"""
    INVALID_ARGUMENT = 3
    """Argument invalid regardless of system state"""
    DEADLINE_EXCEEDED = 4
    """Operation expired, may or may not have completed."""
    NOT_FOUND = 5
    """Entity not found."""
    ALREADY_EXISTS = 6
    """Entity already exists."""
    PERMISSION_DENIED = 7
    """Operation not authorized."""
    RESOURCE_EXHAUSTED = 8
    """Quota exhausted."""
    FAILED_PRECONDITION = 9
    """Argument invalid in current system state."""
    ABORTED = 10
    """Operation aborted."""
    OUT_OF_RANGE = 11
    """Out of bounds, use instead of FailedPrecondition."""
    UNIMPLEMENTED = 12
    """Operation not implemented or disabled."""
    INTERNAL = 13
    """Internal error, reserved for "serious errors"."""
    UNAVAILABLE = 14
    """Unavailable, client should back off and retry."""
    DATA_LOSS = 15
    """Unrecoverable data loss or corruption."""
    UNAUTHENTICATED = 16
    """Request isn't authenticated."""

    def string(self) -> str:
        """Return a string representation of the Code enum value.

        This method matches the current instance of the Code enum and returns
        a corresponding string representation for each possible value.

        Returns:
            str: The string representation of the Code enum value.

        Possible return values:
            - "canceled"
            - "unknown"
            - "invalid_argument"
            - "deadline_exceeded"
            - "not_found"
            - "already_exists"
            - "permission_denied"
            - "resource_exhausted"
            - "failed_precondition"
            - "aborted"
            - "out_of_range"
            - "unimplemented"
            - "internal"
            - "unavailable"
            - "data_loss"
            - "unauthenticated"
            - "code_{self}": For any other value not explicitly matched.
        """
        match self:
            case Code.CANCELED:
                return "canceled"
            case Code.UNKNOWN:
                return "unknown"
            case Code.INVALID_ARGUMENT:
                return "invalid_argument"
            case Code.DEADLINE_EXCEEDED:
                return "deadline_exceeded"
            case Code.NOT_FOUND:
                return "not_found"
            case Code.ALREADY_EXISTS:
                return "already_exists"
            case Code.PERMISSION_DENIED:
                return "permission_denied"
            case Code.RESOURCE_EXHAUSTED:
                return "resource_exhausted"
            case Code.FAILED_PRECONDITION:
                return "failed_precondition"
            case Code.ABORTED:
                return "aborted"
            case Code.OUT_OF_RANGE:
                return "out_of_range"
            case Code.UNIMPLEMENTED:
                return "unimplemented"
            case Code.INTERNAL:
                return "internal"
            case Code.UNAVAILABLE:
                return "unavailable"
            case Code.DATA_LOSS:
                return "data_loss"
            case Code.UNAUTHENTICATED:
                return "unauthenticated"
            case _:
                return f"code_{self}"
