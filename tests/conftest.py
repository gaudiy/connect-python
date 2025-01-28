# ruff: noqa: ARG001 ARG002 D100 D101 D102 D103 D107

import asyncio
import json
import multiprocessing
import platform
import signal
import socket
import threading
import time
import typing
from functools import partial
from multiprocessing.connection import wait
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Event as EventType
from pickle import PicklingError
from urllib.parse import parse_qsl

import hypercorn
import hypercorn.app_wrappers
import hypercorn.asyncio.run
import hypercorn.trio
import hypercorn.typing
import hypercorn.utils
import pytest
from anyio import sleep
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
        body = b""
        more_body = True

        while more_body:
            message = await self.receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)

        return body

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
    app = request.param if callable(request.param) else DefaultApp

    config = Config(app=app, lifespan="off", loop="asyncio")
    server = TestServer(config=config)
    yield from serve_in_thread(server)


async def app(scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    response = PingResponse(name="test").SerializeToString()

    await send({"type": "http.response.body", "body": response})


def app_worker(
    app: hypercorn.typing.AppWrapper,
    config: hypercorn.config.Config,
    sockets: hypercorn.config.Sockets | None = None,
    shutdown_event: EventType | None = None,
) -> None:
    shutdown_trigger = None
    if shutdown_event is not None:
        shutdown_trigger = partial(
            hypercorn.utils.check_multiprocess_shutdown_event,
            shutdown_event,
            asyncio.sleep,
        )

    hypercorn.asyncio.run._run(
        partial(hypercorn.asyncio.run.worker_serve, app, config, sockets=sockets),
        debug=config.debug,
        shutdown_trigger=shutdown_trigger,
    )


class HypercornServer:
    active: bool
    config: hypercorn.config.Config
    ctx: BaseContext
    shutdown_event: EventType
    processes: list[BaseProcess]
    sockets: hypercorn.config.Sockets
    app_wrapper: hypercorn.typing.AppWrapper
    exitcode: int

    def __init__(self, config: hypercorn.config.Config, app: hypercorn.typing.Framework) -> None:
        self.config = config
        self.app = app
        self.active = True
        self.ctx = multiprocessing.get_context("spawn")
        self.shutdown_event = self.ctx.Event()
        self.processes = []
        self.sockets = config.create_sockets()
        self.app_wrapper = hypercorn.utils.wrap_app(app, config.wsgi_max_body_size, "asgi")
        self.exitcode = 0

    @property
    def url(self) -> str:
        protocol = "https" if self.config.ssl_enabled else "http"
        return f"{protocol}://{self.config.bind[0]}"

    def shutdown(self, *args: typing.Any) -> None:
        self.shutdown_event.set()
        self.active = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        _populate(
            self.processes,
            self.app_wrapper,
            self.config,
            app_worker,
            self.sockets,
            self.shutdown_event,
            self.ctx,
        )

        for signal_name in {"SIGINT", "SIGTERM", "SIGBREAK"}:
            if hasattr(signal, signal_name):
                signal.signal(getattr(signal, signal_name), self.shutdown)

    def serve_forever(self) -> None:
        while self.active:
            self.run()
            self.wait_for_closing()

        for process in self.processes:
            process.terminate()

        exitcode = _join_exited(self.processes) if self.exitcode != 0 else self.exitcode
        self.exitcode = exitcode

        for sock in self.sockets.secure_sockets:
            sock.close()

        for sock in self.sockets.insecure_sockets:
            sock.close()

    def wait_for_closing(self) -> None:
        wait(process.sentinel for process in self.processes)

        exitcode = _join_exited(self.processes)
        if exitcode != 0:
            self.shutdown_event.set()
            self.active = False

        self.exitcode = exitcode


def serve_in_multiprocess(server: HypercornServer) -> typing.Iterator[HypercornServer]:
    try:
        server.run()
        while not server.active and not all(process.is_alive() for process in server.processes):
            time.sleep(1e-3)
        yield server
    finally:
        server.shutdown()
        server.wait_for_closing()

        for process in server.processes:
            process.terminate()

        exitcode = _join_exited(server.processes) if server.exitcode != 0 else server.exitcode
        server.exitcode = exitcode

        for sock in server.sockets.secure_sockets:
            sock.close()

        for sock in server.sockets.insecure_sockets:
            sock.close()


@pytest.fixture(scope="session")
def hypercorn_server(request: pytest.FixtureRequest) -> typing.Iterator[HypercornServer]:
    config = hypercorn.config.Config()
    app = request.param if callable(request.param) else DefaultApp

    server = HypercornServer(config, typing.cast(hypercorn.typing.ASGIFramework, app))
    yield from serve_in_multiprocess(server)


def _populate(
    processes: list[BaseProcess],
    app_wrapper: hypercorn.typing.AppWrapper,
    config: hypercorn.config.Config,
    worker_func: typing.Callable[..., None],
    sockets: hypercorn.config.Sockets,
    shutdown_event: EventType,
    ctx: BaseContext,
) -> None:
    for _ in range(config.workers - len(processes)):
        process = ctx.Process(  # type: ignore
            target=worker_func,
            kwargs={
                "app": app_wrapper,
                "config": config,
                "shutdown_event": shutdown_event,
                "sockets": sockets,
            },
        )
        process.daemon = True
        try:
            process.start()
        except PicklingError as error:
            raise RuntimeError(
                "Cannot pickle the config, see https://docs.python.org/3/library/pickle.html#pickle-picklable"  # noqa: E501
            ) from error
        processes.append(process)
        if platform.system() == "Windows":
            time.sleep(0.1)


def _join_exited(processes: list[BaseProcess]) -> int:
    exitcode = 0
    for index in reversed(range(len(processes))):
        worker = processes[index]
        if worker.exitcode is not None:
            worker.join()
            exitcode = worker.exitcode if exitcode == 0 else exitcode
            del processes[index]

    return exitcode
