"""Fixtures for the nats custom integration tests.

Runs a real nats-server (with JetStream) on a free localhost port per test.
"""

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

NATS_SERVER = shutil.which("nats-server")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"nats-server did not start on port {port}")


REPO_CUSTOM_COMPONENTS = Path(__file__).resolve().parents[1] / "custom_components"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Let Home Assistant load integrations from this repo's custom_components/.

    The test plugin ships its own `custom_components` package, which shadows
    the repo's directory, so add ours to that package's search path.
    """
    import custom_components

    if str(REPO_CUSTOM_COMPONENTS) not in custom_components.__path__:
        custom_components.__path__.append(str(REPO_CUSTOM_COMPONENTS))


class NatsServer:
    """A nats-server subprocess that can be stopped and restarted."""

    def __init__(self, store_dir: Path) -> None:
        self.port = _free_port()
        self.store_dir = store_dir
        self._proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def start(self) -> None:
        self._proc = subprocess.Popen(
            [NATS_SERVER, "-a", "127.0.0.1", "-p", str(self.port),
             "-js", "-sd", str(self.store_dir)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_port(self.port)

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait(timeout=5)
            self._proc = None


@pytest.fixture
def nats_server(tmp_path: Path, socket_enabled) -> Iterator[NatsServer]:
    if NATS_SERVER is None:
        pytest.skip("nats-server binary not installed")
    server = NatsServer(tmp_path / "js")
    server.start()
    yield server
    server.stop()
