"""Provides an asynchronous client session for managing HTTP connections."""

import abc
import ssl
import types
import typing
from types import TracebackType
from typing import Self

import httpcore

from connect.utils import map_httpcore_exceptions


class AbstractAsyncContextManager(abc.ABC):
    """Abstract base class for an asynchronous context manager."""

    async def __aenter__(self) -> Self:
        """Enter the context manager and return the instance."""
        return self

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager and handle any exceptions that occur."""
        return None


class AsyncClientSession(AbstractAsyncContextManager):
    """An asynchronous client session for managing HTTP connections.

    This class provides an asynchronous context manager for managing a pool of HTTP connections.
    It supports both HTTP/1 and HTTP/2 protocols and allows configuration of various connection
    parameters such as SSL context, proxy, maximum connections, keep-alive settings, retries,
    and network backend.

    Attributes:
        pool (httpcore.AsyncConnectionPool): The connection pool used for managing HTTP connections.

    Args:
        ssl_context (ssl.SSLContext | None): The SSL context for secure connections. Defaults to None.
        proxy (httpcore.Proxy | None): The proxy configuration. Defaults to None.
        max_connections (int | None): The maximum number of connections. Defaults to 10.
        max_keepalive_connections (int | None): The maximum number of keep-alive connections. Defaults to None.
        keepalive_expiry (float | None): The keep-alive expiry time in seconds. Defaults to None.
        http1 (bool): Whether to support HTTP/1 protocol. Defaults to True.
        http2 (bool): Whether to support HTTP/2 protocol. Defaults to True.
        retries (int): The number of retries for failed requests. Defaults to 0.
        local_address (str | None): The local address to bind to. Defaults to None.
        uds (str | None): The Unix domain socket to bind to. Defaults to None.
        network_backend (httpcore.AsyncNetworkBackend | None): The network backend to use. Defaults to None.
        socket_options (typing.Iterable[httpcore.SOCKET_OPTION] | None): The socket options to set. Defaults to None.

    """

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        proxy: httpcore.Proxy | None = None,
        max_connections: int | None = 10,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        http1: bool = False,
        http2: bool = True,  # because bidi-streams are not supported in HTTP/1
        retries: int = 0,
        local_address: str | None = None,
        uds: str | None = None,
        network_backend: httpcore.AsyncNetworkBackend | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> None:
        """Initialize the connection pool with the given parameters."""
        self.pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl_context,
            proxy=proxy,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
            http1=http1,
            http2=http2,
            retries=retries,
            local_address=local_address,
            uds=uds,
            network_backend=network_backend,
            socket_options=socket_options,
        )

    async def aclose(self) -> None:
        """Close the connection pool."""
        await self.pool.aclose()

    async def __aenter__(self) -> "AsyncClientSession":
        """Enter the context manager and return the instance."""
        await self.pool.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        """Exit the context manager and handle any exceptions that occur."""
        with map_httpcore_exceptions():
            await self.pool.__aexit__(exc_type, exc, tb)
