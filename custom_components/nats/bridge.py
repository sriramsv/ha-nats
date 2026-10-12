"""Connection to the NATS server: publish, subscriptions and durable consumers."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.errors import Error as NatsError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import (
    CONNECT_TIMEOUT,
    EVENT_NATS_MESSAGE,
    FETCH_BATCH,
    FETCH_TIMEOUT,
    INITIAL_CONNECT_RETRY,
    RECONNECT_WAIT,
    SIGNAL_CONNECTION_STATE,
)

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsumerSpec:
    """A JetStream durable pull consumer whose messages become HA events."""

    stream: str
    durable: str
    filter_subject: str | None = None


@dataclass(frozen=True)
class BridgeConfig:
    """Connection settings and what to listen to."""

    url: str
    user: str | None
    password: str | None
    client_name: str
    subscribe: tuple[str, ...]
    consume: tuple[ConsumerSpec, ...]


def decode_payload(data: bytes) -> Any:
    """Return parsed JSON when the payload is JSON, else the decoded text."""
    text = data.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def encode_payload(payload: Any) -> bytes:
    """Strings go out as-is; everything else is JSON-encoded."""
    if payload is None:
        return b""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class CannotConnect(Exception):
    """The server couldn't be reached."""


class InvalidAuth(Exception):
    """The server rejected the credentials."""


_PUBLISH_DENIED = re.compile(r'permissions violation for publish to "([^"]+)"', re.IGNORECASE)


class PermissionDenied(Exception):
    """The server refused a publish to this subject."""

    def __init__(self, subject: str) -> None:
        super().__init__(subject)
        self.subject = subject


def _is_auth_error(err: BaseException) -> bool:
    return "authorization violation" in str(err).lower()


async def probe_connection(
    url: str, user: str | None, password: str | None, client_name: str
) -> None:
    """Connect once and disconnect; raise InvalidAuth or CannotConnect.

    nats-py's connect() hangs on an unreachable server when reconnects are
    disabled, so allow exactly one retry (it then raises NoServersError) and
    keep an outer timeout as a backstop. Auth failures only show up in the
    error callback, so collect those.
    """
    errors: list[Exception] = []

    async def collect(err: Exception) -> None:
        errors.append(err)

    try:
        async with asyncio.timeout(CONNECT_TIMEOUT * 3):
            client = await nats.connect(
                servers=[url],
                user=user,
                password=password,
                name=client_name,
                connect_timeout=CONNECT_TIMEOUT,
                max_reconnect_attempts=1,
                reconnect_time_wait=0.5,
                error_cb=collect,
            )
    except (NatsError, OSError, TimeoutError) as err:
        if any(_is_auth_error(e) for e in (*errors, err)):
            raise InvalidAuth from err
        raise CannotConnect(str(err) or type(err).__name__) from err
    await client.close()


class NatsBridge:
    """Owns the NATS connection and the background tasks around it."""

    def __init__(
        self,
        hass: HomeAssistant,
        config: BridgeConfig,
        on_auth_failed: Callable[[], None] | None = None,
    ) -> None:
        self._hass = hass
        self._config = config
        self._on_auth_failed = on_auth_failed
        self._auth_failure_reported = False
        # Subjects the server refused since their last publish. The server
        # reports a denied PUB asynchronously, through the error callback.
        self._denied_publishes: set[str] = set()
        self._client: Client | None = None
        self._tasks: set[asyncio.Task] = set()
        self._stopping = False

    @property
    def connected(self) -> bool:
        return self._client is not None and self._client.is_connected

    # -- lifecycle ---------------------------------------------------------

    @callback
    def start(self) -> None:
        """Connect in the background so HA setup never waits on NATS."""
        self._spawn(self._connect_until_up(), "nats connect")

    async def stop(self) -> None:
        self._stopping = True
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._client is not None and not self._client.is_closed:
            try:
                await self._client.close()
            except asyncio.CancelledError:
                # With a JetStream fetch pending, nats-py's close() can raise a
                # stray CancelledError from the transport's wait_closed(). Only
                # propagate it if this task itself is being cancelled.
                if (task := asyncio.current_task()) and task.cancelling():
                    raise
                _LOGGER.debug("NATS close interrupted; the connection is closed")
            except NatsError as err:
                _LOGGER.debug("Error closing NATS connection: %s", err)
        self._notify_state()

    def _spawn(self, coro, name: str) -> None:
        task = self._hass.async_create_background_task(coro, name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _connect_until_up(self) -> None:
        while not self._stopping:
            try:
                self._client = await nats.connect(
                    servers=[self._config.url],
                    user=self._config.user,
                    password=self._config.password,
                    name=self._config.client_name,
                    connect_timeout=CONNECT_TIMEOUT,
                    allow_reconnect=True,
                    max_reconnect_attempts=-1,
                    reconnect_time_wait=RECONNECT_WAIT,
                    disconnected_cb=self._on_disconnected,
                    reconnected_cb=self._on_reconnected,
                    error_cb=self._on_error,
                )
            except (NatsError, OSError, asyncio.TimeoutError) as err:
                _LOGGER.warning(
                    "Can't connect to NATS at %s (%s); retrying in %ss",
                    self._config.url, err, INITIAL_CONNECT_RETRY,
                )
                await asyncio.sleep(INITIAL_CONNECT_RETRY)
                continue
            _LOGGER.info("Connected to NATS at %s", self._config.url)
            self._notify_state()
            await self._start_listeners()
            return

    async def _start_listeners(self) -> None:
        for subject in self._config.subscribe:
            await self._client.subscribe(subject, cb=self._on_core_message)
        for spec in self._config.consume:
            self._spawn(self._consume_forever(spec), f"nats consume {spec.durable}")

    # -- connection callbacks ---------------------------------------------
    # Logged at WARNING at most: an ERROR here would trip the automation
    # error watchdog's system_log_event trigger.

    async def _on_disconnected(self) -> None:
        if not self._stopping:
            _LOGGER.warning("Disconnected from NATS; reconnecting")
        self._notify_state()

    async def _on_reconnected(self) -> None:
        _LOGGER.info("Reconnected to NATS")
        self._auth_failure_reported = False
        self._notify_state()

    async def _on_error(self, err: Exception) -> None:
        if denied := _PUBLISH_DENIED.search(str(err)):
            self._denied_publishes.add(denied.group(1))
            _LOGGER.debug("NATS server refused a publish: %s", err)
            return
        if _is_auth_error(err) and not self._auth_failure_reported:
            self._auth_failure_reported = True
            _LOGGER.warning("NATS server at %s rejected the credentials", self._config.url)
            if self._on_auth_failed is not None:
                self._on_auth_failed()
            return
        _LOGGER.debug("NATS client error: %s", err)

    def _notify_state(self) -> None:
        async_dispatcher_send(self._hass, SIGNAL_CONNECTION_STATE, self.connected)

    # -- inbound -----------------------------------------------------------

    async def _on_core_message(self, msg: Msg) -> None:
        self._fire(msg, {})

    def _fire(self, msg: Msg, extra: dict[str, Any]) -> None:
        self._hass.bus.async_fire(
            EVENT_NATS_MESSAGE,
            {
                "subject": msg.subject,
                "data": decode_payload(msg.data),
                "headers": dict(msg.headers) if msg.headers else None,
                **extra,
            },
        )

    async def _consume_forever(self, spec: ConsumerSpec) -> None:
        subscription = None
        while not self._stopping:
            try:
                if subscription is None:
                    subscription = await self._bind_consumer(spec)
                messages = await subscription.fetch(FETCH_BATCH, timeout=FETCH_TIMEOUT)
            except (NatsTimeoutError, asyncio.TimeoutError):
                continue
            except NatsError as err:
                _LOGGER.warning("NATS consumer %s/%s: %s", spec.stream, spec.durable, err)
                await asyncio.sleep(RECONNECT_WAIT)
                continue
            for msg in messages:
                await self._deliver(spec, msg)

    async def _deliver(self, spec: ConsumerSpec, msg: Msg) -> None:
        # Fire before acking: if HA dies in between, the message is redelivered.
        self._fire(msg, {"stream": spec.stream, "seq": msg.metadata.sequence.stream})
        try:
            await msg.ack()
        except NatsError as err:
            _LOGGER.warning("Couldn't ack %s seq %s: %s",
                            spec.stream, msg.metadata.sequence.stream, err)

    async def _bind_consumer(self, spec: ConsumerSpec):
        js = self._client.jetstream()
        try:
            await js.consumer_info(spec.stream, spec.durable)
        except NotFoundError:
            await js.add_consumer(
                spec.stream,
                ConsumerConfig(
                    durable_name=spec.durable,
                    ack_policy=AckPolicy.EXPLICIT,
                    deliver_policy=DeliverPolicy.ALL,
                    filter_subject=spec.filter_subject,
                ),
            )
        return await js.pull_subscribe_bind(durable=spec.durable, stream=spec.stream)

    def diagnostics(self) -> dict[str, Any]:
        """Connection details for the diagnostics download (no secrets)."""
        client = self._client
        return {
            "connected": self.connected,
            "server_version": (
                str(client.connected_server_version)
                if client is not None and self.connected else None
            ),
            "subscriptions": list(self._config.subscribe),
            "consumers": [
                {"stream": c.stream, "durable": c.durable, "filter_subject": c.filter_subject}
                for c in self._config.consume
            ],
        }

    # -- outbound ----------------------------------------------------------

    async def publish(
        self,
        subject: str,
        payload: Any,
        headers: dict[str, str] | None,
        jetstream: bool,
    ) -> dict[str, Any]:
        """Publish and return what happened; with jetstream=True, the stream ack.

        Raises PermissionDenied when the server refuses the subject. For a
        core publish the server's -ERR arrives before the PONG that completes
        flush(), and nats-py runs the error callback inline, so the refusal is
        known by the time flush() returns.
        """
        if not self.connected:
            raise NotConnected
        data = encode_payload(payload)
        self._denied_publishes.discard(subject)
        if jetstream:
            try:
                ack = await self._client.jetstream().publish(subject, data, headers=headers)
            except NatsError as err:
                if subject in self._denied_publishes:
                    self._denied_publishes.discard(subject)
                    raise PermissionDenied(subject) from err
                raise
            return {"subject": subject, "jetstream": True, "stream": ack.stream, "seq": ack.seq}
        await self._client.publish(subject, data, headers=headers)
        await self._client.flush(timeout=CONNECT_TIMEOUT)
        if subject in self._denied_publishes:
            self._denied_publishes.discard(subject)
            raise PermissionDenied(subject)
        return {"subject": subject, "jetstream": False}


class NotConnected(Exception):
    """Raised when publishing without a live connection."""
