import logging
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
from concurrent.futures import as_completed
from typing import cast

import anyio
import hypercorn
import hypercorn.asyncio.run
from anyio import from_thread
from gen.connectrpc.conformance.v1 import config_pb2
from gen.connectrpc.conformance.v1.server_compat_pb2 import ServerCompatRequest, ServerCompatResponse
from server import app

logger = logging.getLogger("conformance.runner")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("conformance_server.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def find_free_port() -> int:
    """Find a free port to bind the server to."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def create_ssl_context(request: ServerCompatRequest) -> tuple[str, str, str | None]:
    """Create SSL certificate files and return their paths."""
    # Create temporary files for the certificates

    # Create temporary files that won't be deleted when closed
    temp_dir = tempfile.mkdtemp()

    # Write certificate to file
    cert_path = os.path.join(temp_dir, "cert.pem")
    with open(cert_path, "wb") as f:
        f.write(request.server_creds.cert)

    # Write key to file
    key_path = os.path.join(temp_dir, "key.pem")
    with open(key_path, "wb") as f:
        f.write(request.server_creds.key)

    # If client certificate is required, write it to a file too
    client_ca_path = None
    if request.client_tls_cert:
        client_ca_path = os.path.join(temp_dir, "client_ca.pem")
        with open(client_ca_path, "wb") as f:
            f.write(request.client_tls_cert)

    return cert_path, key_path, client_ca_path


def start_server(request: ServerCompatRequest) -> ServerCompatResponse:
    # Find a free port
    port = find_free_port()
    host = "127.0.0.1"

    config = hypercorn.Config()
    config.bind = [f"{host}:{port}"]

    if request.http_version == config_pb2.HTTP_VERSION_1:
        config.alpn_protocols = ["http/1.1"]
    else:  # Defaults to HTTP/2
        config.alpn_protocols = ["h2", "http/1.1"]

    # Configure TLS if needed
    if request.use_tls:
        cert_path, key_path, ca_certs_path = create_ssl_context(request)
        config.certfile = cert_path
        config.keyfile = key_path
        if ca_certs_path:
            config.ca_certs = ca_certs_path
            config.verify_mode = ssl.CERT_REQUIRED

    response = ServerCompatResponse(
        host=host, port=port, pem_cert=request.server_creds.cert if request.use_tls else None
    )

    def notify_caller() -> None:
        time.sleep(0.1)
        write_message_to_stdout(response)

    threading.Thread(target=notify_caller).start()

    shutdown_event = anyio.Event()

    async def _start_server(
        config: hypercorn.config.Config,
        app: hypercorn.typing.ASGIFramework,
        shutdown_event: anyio.Event,
    ) -> None:
        if not shutdown_event.is_set():
            await hypercorn.asyncio.serve(app, config, shutdown_trigger=shutdown_event.wait)

    with from_thread.start_blocking_portal() as portal:
        future = portal.start_task_soon(
            _start_server,
            config,
            cast(hypercorn.typing.ASGIFramework, app),
            shutdown_event,
        )

        for f in as_completed([future]):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error starting server: {e}", exc_info=True)
                raise

    return response


def read_message_from_stdin() -> ServerCompatRequest:
    try:
        request_size = int.from_bytes(sys.stdin.buffer.read(4), byteorder="big")
        request_buf = sys.stdin.buffer.read(request_size)
        request = ServerCompatRequest.FromString(request_buf)
        return request
    except Exception as e:
        logger.error(f"Error reading message from stdin: {e}", exc_info=True)
        raise


def write_message_to_stdout(response: ServerCompatResponse) -> None:
    response_buf = response.SerializeToString()
    response_size = len(response_buf)
    sys.stdout.buffer.write(response_size.to_bytes(length=4, byteorder="big"))
    sys.stdout.buffer.write(response_buf)
    sys.stdout.buffer.flush()


def main() -> None:
    try:
        request = read_message_from_stdin()

        start_server(request)

    except EOFError:
        logger.info("EOF reached, stopping server.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
