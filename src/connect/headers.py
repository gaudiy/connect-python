# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Provides a `Headers` class for managing HTTP headers and a utility function for request headers."""

from collections.abc import AsyncIterable, Iterable, Iterator, KeysView, Mapping, MutableMapping, Sequence
from typing import Any, Union

from yarl import URL

DEFAULT_PORTS = {
    b"ftp": 21,
    b"http": 80,
    b"https": 443,
    b"ws": 80,
    b"wss": 443,
}


HeaderTypes = Union[
    "Headers",
    Mapping[str, str],
    Mapping[bytes, bytes],
    Sequence[tuple[str, str]],
    Sequence[tuple[bytes, bytes]],
]


def _normalize_header_key(key: str | bytes, encoding: str | None = None) -> bytes:
    return key if isinstance(key, bytes) else key.encode(encoding or "ascii")


def _normalize_header_value(value: str | bytes, encoding: str | None = None) -> bytes:
    if isinstance(value, bytes):
        return value

    try:
        return value.encode(encoding or "ascii")
    except UnicodeEncodeError as e:
        raise TypeError(f"Header values must be of type bytes or ASCII str, not {type(value).__name__}") from e


class Headers(MutableMapping[str, str]):
    """A case-insensitive, multi-valued dictionary for HTTP headers.

    This class provides a dictionary-like interface for managing HTTP headers,
    with special handling for their unique characteristics. Keys are treated in a
    case-insensitive manner, and it's possible to have multiple values for the
    same key, which are handled gracefully.

    Internally, headers are stored as bytes to manage character encodings
    correctly. The class can auto-detect the encoding or use a specified one.

    When accessing a header that has multiple values via standard dictionary
    lookup (`headers['key']`), the values are concatenated into a single,
    comma-separated string. To retrieve all key-value pairs, including
    duplicates, the `multi_items()` method should be used.

    It inherits from `collections.abc.MutableMapping`, providing standard
    dictionary methods like `keys()`, `items()`, `__getitem__`, `__setitem__`,
    and `__delitem__`.

    Attributes:
        encoding (str): The character encoding used for header keys and values.
            It can be set manually or is auto-detected.
        raw (list[tuple[bytes, bytes]]): A list of the raw (key, value) byte
            pairs, preserving the original case of the keys.
    """

    _list: list[tuple[bytes, bytes, bytes]]

    def __init__(
        self,
        headers: HeaderTypes | None = None,
        encoding: str | None = None,
    ) -> None:
        """Initializes a new Headers object.

        Args:
            headers: An optional initial set of headers. Can be provided as
                another `Headers` instance, a mapping (e.g., a dictionary),
                or an iterable of (key, value) tuples.
            encoding: The character encoding to use for converting string
                header keys and values to bytes.
        """
        self._list = []

        if isinstance(headers, Headers):
            self._list = list(headers._list)
        elif isinstance(headers, Mapping):
            for k, v in headers.items():
                bytes_key = _normalize_header_key(k, encoding)
                bytes_value = _normalize_header_value(v, encoding)
                self._list.append((bytes_key, bytes_key.lower(), bytes_value))
        elif headers is not None:
            for k, v in headers:
                bytes_key = _normalize_header_key(k, encoding)
                bytes_value = _normalize_header_value(v, encoding)
                self._list.append((bytes_key, bytes_key.lower(), bytes_value))

        self._encoding = encoding

    @property
    def raw(self) -> list[tuple[bytes, bytes]]:
        """Get the raw headers as a list of key-value pairs.

        Returns:
            list[tuple[bytes, bytes]]: A list of (key, value) tuples,
            where both the key and the value are bytes.
        """
        return [(raw_key, value) for raw_key, _, value in self._list]

    def keys(self) -> KeysView[str]:
        """Return a new view of the header keys.

        The keys are decoded from bytes into strings using the configured encoding.

        Returns:
            A view object displaying a list of all header keys.
        """
        return {key.decode(self.encoding): None for _, key, value in self._list}.keys()

    @property
    def encoding(self) -> str:
        """Determine and return the encoding for the headers.

        The method iterates through a list of preferred encodings ('ascii', 'utf-8')
        and attempts to decode all header keys and values. The first encoding
        that successfully decodes all headers without a `UnicodeDecodeError` is
        chosen and cached for subsequent calls.

        If neither 'ascii' nor 'utf-8' is suitable, it falls back to 'iso-8859-1',
        which can represent any byte value and is thus a safe default.

        Returns:
            str: The name of the determined encoding for the headers.

        """
        if self._encoding is None:
            for encoding in ["ascii", "utf-8"]:
                for key, value in self.raw:
                    try:
                        key.decode(encoding)
                        value.decode(encoding)
                    except UnicodeDecodeError:
                        break
                else:
                    # The else block runs if 'break' did not occur, meaning
                    # all values fitted the encoding.
                    self._encoding = encoding
                    break
            else:
                # The ISO-8859-1 encoding covers all 256 code points in a byte,
                # so will never raise decode errors.
                self._encoding = "iso-8859-1"
        return self._encoding

    @encoding.setter
    def encoding(self, value: str) -> None:
        self._encoding = value

    def copy(self) -> "Headers":
        """Returns a copy of the Headers object.

        Returns:
            Headers: A new Headers instance.
        """
        return Headers(self, encoding=self.encoding)

    def multi_items(self) -> list[tuple[str, str]]:
        """Returns a list of all header key-value pairs.

        The keys and values are decoded to strings using the specified encoding.
        This method is useful for headers that can appear multiple times,
        as it returns all occurrences.

        Returns:
            list[tuple[str, str]]: A list of (key, value) tuples.
        """
        return [(key.decode(self.encoding), value.decode(self.encoding)) for _, key, value in self._list]

    def __getitem__(self, key: str) -> str:
        """Retrieves a header value by its case-insensitive key.

        If multiple headers share the same key, their values are concatenated
        into a single string, separated by a comma and a space.

        Args:
            key: The case-insensitive name of the header to retrieve.

        Returns:
            The corresponding header value.

        Raises:
            KeyError: If no header with the given key is found.
        """
        normalized_key = key.lower().encode(self.encoding)

        items = [
            header_value.decode(self.encoding)
            for _, header_key, header_value in self._list
            if header_key == normalized_key
        ]

        if items:
            return ", ".join(items)

        raise KeyError(key)

    def __setitem__(self, key: str, value: str) -> None:
        """Sets a header value, treating the header name case-insensitively.

        If a header with the same name (case-insensitively) already exists,
        its value is updated. The original casing of the new key is preserved.

        If multiple headers with the same name exist, all subsequent occurrences
        are removed, and the first one is updated with the new value. If the
        header does not exist, it is added.

        Args:
            key: The name of the header.
            value: The value for the header.
        """
        set_key = key.encode(self._encoding or "utf-8")
        set_value = value.encode(self._encoding or "utf-8")
        lookup_key = set_key.lower()

        found_indexes = [idx for idx, (_, item_key, _) in enumerate(self._list) if item_key == lookup_key]

        for idx in reversed(found_indexes[1:]):
            del self._list[idx]

        if found_indexes:
            idx = found_indexes[0]
            self._list[idx] = (set_key, lookup_key, set_value)
        else:
            self._list.append((set_key, lookup_key, set_value))

    def __delitem__(self, key: str) -> None:
        """Delete all headers matching the given key.

        The key matching is case-insensitive. If multiple headers have the same
        name, all of them will be removed.

        Args:
            key: The case-insensitive name of the header(s) to remove.

        Raises:
            KeyError: If no header with the given key is found.
        """
        del_key = key.lower().encode(self.encoding)

        pop_indexes = [idx for idx, (_, item_key, _) in enumerate(self._list) if item_key.lower() == del_key]

        if not pop_indexes:
            raise KeyError(key)

        for idx in reversed(pop_indexes):
            del self._list[idx]

    def __iter__(self) -> Iterator[Any]:
        """Return an iterator over the keys of the dictionary.

        Yields:
            Iterator[Any]: An iterator over the keys of the dictionary.

        """
        return iter(self.keys())

    def __len__(self) -> int:
        """Return the number of items in the list.

        Returns:
            int: The number of items in the list.

        """
        return len(self._list)


def include_request_headers(
    headers: Headers,
    url: URL,
    content: bytes | Iterable[bytes] | AsyncIterable[bytes] | None,
    method: str | None = None,
) -> Headers:
    """Adds required request headers like 'Host' and 'Content-Length' if not present.

    This function inspects the request details (URL, content, method) and
    populates essential HTTP headers if they are missing.

    - It sets the 'Host' header based on the target URL, including the port
      if it's non-standard.
    - It determines whether to use 'Content-Length' (for fixed-size byte content)
      or 'Transfer-Encoding: chunked' (for streaming content) for the request
      body. This is skipped for 'GET' requests or if these headers are already set.

    Args:
        headers: The mutable headers dictionary-like object for the request.
        url: The URL object of the request, used to determine the 'Host' header.
        content: The request body, which can be bytes, an iterable of bytes,
            or an async iterable of bytes.
        method: The HTTP method of the request (e.g., "GET", "POST").

    Returns:
        The updated headers object.
    """
    if headers.get("Host") is None:
        default_port = DEFAULT_PORTS.get(url.scheme.encode())
        if url.host is None:
            host = "localhost"
        else:
            host = url.host if url.port is None or url.port == default_port else f"{url.host}:{url.port}"

        headers["Host"] = host

    if (
        content is not None
        and headers.get("Content-Length") is None
        and headers.get("Transfer-Encoding") is None
        and method != "GET"
    ):
        if isinstance(content, bytes):
            content_length = str(len(content))
            headers["Content-Length"] = content_length
        elif isinstance(content, Iterable | AsyncIterable):
            headers["Transfer-Encoding"] = "chunked"

    return headers
