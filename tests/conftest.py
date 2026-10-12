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
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

NATS_SERVER = shutil.which("nats-server")
DOMAIN = "nats"


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

    def __init__(
        self, store_dir: Path, user: str | None = None, password: str | None = None
    ) -> None:
        self.port = _free_port()
        self.store_dir = store_dir
        self.user = user
        self.password = password
        self._proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def start(self, password: str | None = None) -> None:
        """Start the server; `password` replaces the configured one."""
        if password is not None:
            self.password = password
        auth = ["--user", self.user, "--pass", self.password] if self.user else []
        self._proc = subprocess.Popen(
            [NATS_SERVER, "-a", "127.0.0.1", "-p", str(self.port),
             "-js", "-sd", str(self.store_dir), *auth],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_port(self.port)

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait(timeout=5)
            self._proc = None


def _server(tmp_path: Path, **auth) -> Iterator[NatsServer]:
    if NATS_SERVER is None:
        pytest.skip("nats-server binary not installed")
    server = NatsServer(tmp_path / "js", **auth)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def nats_server(tmp_path: Path, socket_enabled) -> Iterator[NatsServer]:
    yield from _server(tmp_path)


@pytest.fixture
def nats_auth_server(tmp_path: Path, socket_enabled) -> Iterator[NatsServer]:
    yield from _server(tmp_path, user="ha", password="secret")


async def setup_entry(
    hass: HomeAssistant,
    url: str,
    *,
    username: str | None = None,
    password: str | None = None,
    **options,
) -> MockConfigEntry:
    """Add and set up a config entry for `url` with the given options."""
    data = {"url": url, "client_name": "home-assistant"}
    if username is not None:
        data |= {"username": username, "password": password}
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=url,
        data=data,
        options={"subscribe": [], "consume": [], **options},
    )
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry
