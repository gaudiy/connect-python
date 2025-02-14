# ruff: noqa: ARG001 ARG002 D100 D101 D102 D103 D107

import asyncio
import json
import logging
import socket
import threading
import time
import typing
from urllib.parse import parse_qsl

import anyio
import hypercorn.asyncio.run
import hypercorn.logging
import hypercorn.typing
import pytest
from anyio import from_thread, sleep
from google.protobuf import json_format
from uvicorn.config import Config
from uvicorn.server import Server
from yarl import URL

from tests.testdata.ping.v1.ping_pb2 import PingResponse

Message = typing.MutableMapping[str, typing.Any]
Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[Message], typing.Coroutine[None, None, None]]
Scope = dict[str, typing.Any]


class ASGIRequest:
    scope: Scope
    receive: Receive

    def __init__(self, scope: Scope, receive: Receive) -> None:
        self.scope = scope
        self.receive = receive

    async def body(self) -> bytes:
        return b"".join([message async for message in self.iter_bytes()])

    async def iter_bytes(self) -> typing.AsyncGenerator[bytes]:
        more_body = True
        while more_body:
            message = await self.receive()
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            yield body

    @property
    def headers(self) -> dict[str, str]:
        return {key.decode("latin-1"): value.decode("latin-1") for key, value in self.scope["headers"]}

    @property
    def query_params(self) -> dict[str, str]:
        query_string = self.scope.get("query_string", b"")
        return dict(parse_qsl(query_string.decode("latin-1"), keep_blank_values=True))


class DefaultApp:
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"

        if scope["path"].endswith("/json"):
            await self.ping_json(scope, receive, send)
        elif scope["path"].endswith("/proto"):
            await self.ping_proto(scope, receive, send)
        else:
            await self.not_found(scope, receive, send)

    async def ping_json(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/proto"]],
        })

        content = json_format.MessageToDict(PingResponse(name="test"))

        await send({
            "type": "http.response.body",
            "body": json.dumps(
                content,
                ensure_ascii=False,
                allow_nan=False,
                indent=None,
                separators=(",", ":"),
            ).encode("utf-8"),
        })

    async def ping_proto(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/proto"]],
        })

        content = PingResponse(name="test").SerializeToString()

        await send({"type": "http.response.body", "body": content})

    async def not_found(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            "type": "http.response.start",
            "status": 404,
            "headers": [[b"content-type", b"text/plain"]],
        })

        await send({"type": "http.response.body", "body": b"Not Found"})


class TestServer(Server):
    @property
    def _base_url(self) -> URL:
        protocol = "https" if self.config.is_ssl else "http"
        return URL(f"{protocol}://{self.config.host}:{self.config.port}")

    def make_url(self, path: str) -> str:
        if path.startswith("/"):
            path = path[1:]
        elif path.endswith("/"):
            path = path[:-1]

        return str(self._base_url.joinpath(path))

    async def serve(self, sockets: list[socket.socket] | None = None) -> None:
        self.restart_requested = asyncio.Event()

        loop = asyncio.get_event_loop()
        tasks = {
            loop.create_task(super().serve(sockets=sockets)),
            loop.create_task(self.watch_restarts()),
        }
        await asyncio.wait(tasks)

    async def restart(self) -> None:
        # This coroutine may be called from a different thread than the one the
        # server is running on, and from an async environment that's not asyncio.
        # For this reason, we use an event to coordinate with the server
        # instead of calling shutdown()/startup() directly, and should not make
        # any asyncio-specific operations.
        self.started = False
        self.restart_requested.set()
        while not self.started:
            await sleep(0.2)

    async def watch_restarts(self) -> None:
        while True:
            if self.should_exit:
                return

            try:
                await asyncio.wait_for(self.restart_requested.wait(), timeout=0.1)
            except TimeoutError:
                continue

            self.restart_requested.clear()
            await self.shutdown()
            await self.startup()


def serve_in_thread(server: TestServer) -> typing.Iterator[TestServer]:
    thread = threading.Thread(target=server.run)
    thread.start()
    try:
        while not server.started:
            time.sleep(1e-3)
        yield server
    finally:
        server.should_exit = True
        thread.join()


@pytest.fixture(scope="session")
def server(request: pytest.FixtureRequest) -> typing.Iterator[TestServer]:
    app = request.param if callable(request.param) else DefaultApp()

    config = Config(app=app, lifespan="off", loop="asyncio")
    server = TestServer(config=config)
    yield from serve_in_thread(server)


class ServerConfig(typing.NamedTuple):
    scheme: str
    host: str
    port: int

    @property
    def base_url(self) -> str:
        host = self.host
        if ":" in host:
            host = f"[{host}]"
        return f"{self.scheme}://{host}:{self.port}"


class ExtractURLLogger(hypercorn.logging.Logger):
    url: URL | None

    def __init__(self, config: hypercorn.config.Config) -> None:
        super().__init__(config)
        self.url = None

    async def info(self, message: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        import re

        if self.error_logger is not None:
            self.error_logger.info(message, *args, **kwargs)

        # Extract the URL from the log message. This is a bit of a hack, but it works.
        # e.g. "[INFO] Running on http://127.0.0.1:52282 (CTRL + C to quit)"
        match = re.search(r"(https?://[\w|\.]+:\d{2,5})", message)
        if match:
            url = match.group(0)
            self.url = URL(url)


async def _start_server(
    config: hypercorn.config.Config,
    app: hypercorn.typing.ASGIFramework,
    shutdown_event: anyio.Event,
) -> None:
    if not shutdown_event.is_set():
        await hypercorn.asyncio.serve(app, config, shutdown_trigger=shutdown_event.wait)


def run_hypercorn_in_thread(
    app: hypercorn.typing.ASGIFramework, config: hypercorn.config.Config
) -> typing.Iterator[ServerConfig]:
    config.bind = ["localhost:0"]
    logging.disable(logging.WARNING)

    logger = ExtractURLLogger(config)
    config._log = logger

    shutdown_event = anyio.Event()

    with from_thread.start_blocking_portal() as portal:
        future = portal.start_task_soon(
            _start_server,
            config,
            app,
            shutdown_event,
        )
        try:
            start_time = time.time()
            timeout = 3
            while not logger.url:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Server did not start within {timeout} seconds")

                time.sleep(1e-3)

            cfg = ServerConfig(
                scheme=logger.url.scheme,
                host=logger.url.host or "localhost",
                port=logger.url.port or 80,
            )
            yield cfg
        finally:
            portal.call(shutdown_event.set)
            future.result()


@pytest.fixture(scope="session")
def hypercorn_server(request: pytest.FixtureRequest) -> typing.Iterator[ServerConfig]:
    app = request.param if callable(request.param) else DefaultApp()

    config = hypercorn.config.Config()

    yield from run_hypercorn_in_thread(typing.cast(hypercorn.typing.ASGIFramework, app), config)
