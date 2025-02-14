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
    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        proxy: httpcore.Proxy | None = None,
        max_connections: int | None = 10,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        http1: bool = True,
        http2: bool = True,  # because bidi-streams are not supported in HTTP/1
        retries: int = 0,
        local_address: str | None = None,
        uds: str | None = None,
        network_backend: httpcore.AsyncNetworkBackend | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> None:
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
        await self.pool.aclose()

    async def __aenter__(self) -> "AsyncClientSession":
        await self.pool.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: types.TracebackType | None = None,
    ) -> None:
        with map_httpcore_exceptions():
            await self.pool.__aexit__(exc_type, exc, tb)
