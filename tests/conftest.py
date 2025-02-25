# ruff: noqa: ARG001 ARG002 D100 D101 D102 D103 D107

import json
import logging
import time
import typing
from urllib.parse import parse_qsl

import anyio
import hypercorn.asyncio.run
import hypercorn.logging
import hypercorn.typing
import pytest
from anyio import from_thread
from async_asgi_testclient import TestClient as AsyncTestClient
from google.protobuf import json_format
from starlette.applications import Starlette
from starlette.middleware import Middleware
from yarl import URL

from connect.envelope import Envelope, EnvelopeFlags
from connect.middleware import ConnectMiddleware
from connect.options import ConnectOptions
from tests.testdata.ping.v1.ping_pb2 import PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import (
    PingService_service_descriptor,
    PingServiceHandler,
    create_PingService_handlers,
)

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
        elif scope["path"].endswith("/stream"):
            await self.ping_stream(scope, receive, send)
        else:
            await self.not_found(scope, receive, send)

    async def ping_json(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/proto"]],
        })

        request = ASGIRequest(scope, receive)
        _ = await request.body()

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

        request = ASGIRequest(scope, receive)
        _ = await request.body()

        content = PingResponse(name="test").SerializeToString()

        await send({"type": "http.response.body", "body": content})

    async def ping_stream(self, scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"application/connect+proto"],
                [b"connect-accept-encoding", b"identity"],
                [b"connect-content-encoding", b"identity"],
            ],
        })

        request = ASGIRequest(scope, receive)
        _ = await request.body()

        env = Envelope(PingResponse(name="ping").SerializeToString(), EnvelopeFlags(0))
        await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

        env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
        await send({"type": "http.response.body", "body": env.encode(), "more_body": False})

    async def not_found(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            "type": "http.response.start",
            "status": 404,
            "headers": [[b"content-type", b"text/plain"]],
        })

        await send({"type": "http.response.body", "body": b"Not Found"})


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


class AsyncClient:
    service: PingServiceHandler
    client: AsyncTestClient

    def __init__(self, service: PingServiceHandler, options: ConnectOptions | None = None) -> None:
        self.service = service
        self.options = options

    async def __aenter__(self) -> AsyncTestClient:
        assert isinstance(self.service, PingServiceHandler)

        options = self.options or ConnectOptions()
        options.descriptor = PingService_service_descriptor

        middleware = [
            Middleware(
                ConnectMiddleware,
                create_PingService_handlers(
                    service=self.service,
                    options=options,
                ),
            )
        ]

        app = Starlette(middleware=middleware)

        # TODO(tsubakiky): Implement a new ASGI client for testing instead of using the AsyncTestClient.
        self.client = AsyncTestClient(app)
        await self.client.__aenter__()

        return self.client

    async def __aexit__(self, exc_type: typing.Any, exc_value: typing.Any, traceback: typing.Any) -> None:
        await self.client.__aexit__(exc_type, exc_value, traceback)
