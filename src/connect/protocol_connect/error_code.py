"""Module for mapping Connect error codes to HTTP status codes."""

from connect.code import Code


def connect_code_to_http(code: Code) -> int:
    """Convert a given `Code` enumeration to its corresponding HTTP status code.

    Args:
        code (Code): The `Code` enumeration value to be converted.

    Returns:
        int: The corresponding HTTP status code.

    The mapping is as follows:
        - Code.CANCELED -> 499
        - Code.UNKNOWN -> 500
        - Code.INVALID_ARGUMENT -> 400
        - Code.DEADLINE_EXCEEDED -> 504
        - Code.NOT_FOUND -> 404
        - Code.ALREADY_EXISTS -> 409
        - Code.PERMISSION_DENIED -> 403
        - Code.RESOURCE_EXHAUSTED -> 429
        - Code.FAILED_PRECONDITION -> 400
        - Code.ABORTED -> 409
        - Code.OUT_OF_RANGE -> 400
        - Code.UNIMPLEMENTED -> 501
        - Code.INTERNAL -> 500
        - Code.UNAVAILABLE -> 503
        - Code.DATA_LOSS -> 500
        - Code.UNAUTHENTICATED -> 401
        - Any other code -> 500

    """
    match code:
        case Code.CANCELED:
            return 499
        case Code.UNKNOWN:
            return 500
        case Code.INVALID_ARGUMENT:
            return 400
        case Code.DEADLINE_EXCEEDED:
            return 504
        case Code.NOT_FOUND:
            return 404
        case Code.ALREADY_EXISTS:
            return 409
        case Code.PERMISSION_DENIED:
            return 403
        case Code.RESOURCE_EXHAUSTED:
            return 429
        case Code.FAILED_PRECONDITION:
            return 400
        case Code.ABORTED:
            return 409
        case Code.OUT_OF_RANGE:
            return 400
        case Code.UNIMPLEMENTED:
            return 501
        case Code.INTERNAL:
            return 500
        case Code.UNAVAILABLE:
            return 503
        case Code.DATA_LOSS:
            return 500
        case Code.UNAUTHENTICATED:
            return 401
        case _:
            return 500
