# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

import enum


@enum.unique
class Code(enum.IntEnum):
    """Connect represents categories of errors as codes.

    Connect represents categories of errors as codes, and each code maps to a
    specific HTTP status code.
    The codes and their semantics were chosen to match gRPC. Only the codes below are valid -
    there are no user-defined codes.

    See the specification at https://connectrpc.com/docs/protocol#error-codes
    for details.

    Attributes:
      CANCELLED: RPC canceled, usually by the caller.
      UNKNOWN: Catch-all for errors of unclear origin and errors without a more appropriate code.
      INVALID_ARGUMENT: Request is invalid, regardless of system state.
      DEADLINE_EXCEEDED: Deadline expired before RPC could complete or before the client received the response.
      NOT_FOUND: User requested a resource (for example, a file or directory) that can't be found.
      ALREADY_EXISTS: Caller attempted to create a resource that already exists.
      PERMISSION_DENIED: Caller isn't authorized to perform the operation.
      UNAUTHENTICATED: Caller doesn't have valid authentication credentials for the operation.
      RESOURCE_EXHAUSTED: Operation can't be completed because some resource is exhausted. Use unavailable if the
        server is temporarily overloaded and the caller should retry later.
      FAILED_PRECONDITION: Operation can't be completed because the system isn't in the required state.
      ABORTED: The operation was aborted, often because of concurrency issues like a database transaction abort.
      UNIMPLEMENTED: The operation isn't implemented, supported, or enabled.
      INTERNAL: An invariant expected by the underlying system has been broken. Reserved for serious errors.
      UNAVAILABLE: The service is currently unavailable, usually transiently. Clients should back off and retry idempotent operations.
      DATA_LOSS: Unrecoverable data loss or corruption.

    """

    CANCELED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16
