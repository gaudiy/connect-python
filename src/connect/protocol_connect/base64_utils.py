"""Base64 utilities for Connect protocol."""

import base64


def decode_base64_with_padding(value: str) -> bytes:
    """Decode base64 string with proper padding.

    Args:
        value: Base64 encoded string that may be missing padding

    Returns:
        Decoded bytes

    Raises:
        Exception: If base64 decoding fails
    """
    padded_value = value + "=" * (-len(value) % 4)
    return base64.b64decode(padded_value.encode())


def decode_urlsafe_base64_with_padding(value: str) -> bytes:
    """Decode URL-safe base64 string with proper padding.

    Args:
        value: URL-safe base64 encoded string that may be missing padding

    Returns:
        Decoded bytes

    Raises:
        Exception: If base64 decoding fails
    """
    padded_value = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded_value)


def encode_base64_without_padding(data: bytes) -> str:
    """Encode bytes to base64 string without padding.

    Args:
        data: Bytes to encode

    Returns:
        Base64 encoded string with padding removed
    """
    return base64.b64encode(data).decode().rstrip("=")
