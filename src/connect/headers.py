"""Provides a Headers class for managing HTTP headers."""

from collections.abc import Iterator, KeysView, Mapping, MutableMapping, Sequence
from typing import Any, Union

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
    """A class to represent HTTP headers.

    Attributes:
        raw : list[tuple[bytes, bytes]]
            A list of tuples containing the raw header keys and values.
        encoding : str
            The encoding used for the headers.

    """

    _list: list[tuple[bytes, bytes, bytes]]

    def __init__(
        self,
        headers: HeaderTypes | None = None,
        encoding: str | None = None,
    ) -> None:
        """Initialize the Headers object.

        Args:
            headers (HeaderTypes | None): Initial headers to populate the object.
                Can be another Headers object, a mapping of header key-value pairs,
                or an iterable of key-value pairs.
            encoding (str | None): The encoding to use for header keys and values.
                If None, defaults to the system's default encoding.

        Returns:
            None

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
        """Return the raw headers as a list of tuples.

        Each tuple contains the raw key and value as bytes.

        Returns:
            list[tuple[bytes, bytes]]: A list of tuples where each tuple contains
            the raw key and value as bytes.

        """
        return [(raw_key, value) for raw_key, _, value in self._list]

    def keys(self) -> KeysView[str]:
        """Return a view of the dictionary's keys.

        This method decodes the keys from the internal list using the specified encoding
        and returns a view of these decoded keys.

        Returns:
            KeysView[str]: A view object that displays a list of the dictionary's keys.

        """
        return {key.decode(self.encoding): None for _, key, value in self._list}.keys()

    @property
    def encoding(self) -> str:
        """Determine and returns the encoding used for the headers.

        This method attempts to decode the headers using "ascii" and "utf-8" encodings.
        If neither of these encodings work, it defaults to "iso-8859-1" which can decode
        any byte sequence.

        Returns:
            str: The encoding used for the headers.

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
        """Return a copy of the Headers object."""
        return Headers(self, encoding=self.encoding)

    def multi_items(self) -> list[tuple[str, str]]:
        """Return a list of tuples containing decoded key-value pairs from the internal list.

        The keys and values are decoded using the specified encoding.

        Returns:
            list[tuple[str, str]]: A list of tuples where each tuple contains a decoded key and value.

        """
        return [(key.decode(self.encoding), value.decode(self.encoding)) for _, key, value in self._list]

    def __getitem__(self, key: str) -> str:
        """Retrieve the value associated with the given key in the headers.

        Args:
            key (str): The key to look up in the headers.

        Returns:
            str: The value(s) associated with the key, joined by ", " if multiple values exist.

        Raises:
            KeyError: If the key is not found in the headers.

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
        """Set the value for a given key in the headers list.

        If the key already exists, update its value. If the key appears multiple times, remove all but the first
        occurrence before updating.

        Args:
            key (str): The header key to set.
            value (str): The header value to set.

        Returns:
            None

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
        """Remove the item with the specified key from the list.

        Args:
            key (str): The key of the item to be removed.

        Raises:
            KeyError: If the key is not found in the list.

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
