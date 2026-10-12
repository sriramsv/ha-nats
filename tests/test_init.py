"""Tests for the nats custom integration, against a real nats-server."""

import asyncio
import json

import nats
import pytest
import voluptuous as vol
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import async_capture_events

DOMAIN = "nats"
SENSOR = "binary_sensor.nats_connected"


async def _setup(hass: HomeAssistant, url: str, **extra) -> None:
    config = {DOMAIN: {"url": url, **extra}}
    assert await async_setup_component(hass, DOMAIN, config)
    await hass.async_block_till_done()


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


async def _wait_connected(hass: HomeAssistant) -> None:
    await _wait_until(lambda: (s := hass.states.get(SENSOR)) and s.state == "on")


async def test_connects_and_reports_connectivity(hass, nats_server):
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)
    assert hass.states.get(SENSOR).attributes["device_class"] == "connectivity"


async def test_publish_dict_payload_as_json(hass, nats_server):
    client = await nats.connect(nats_server.url)
    sub = await client.subscribe("claude.tasks.test")
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)

    await hass.services.async_call(
        DOMAIN, "publish",
        {"subject": "claude.tasks.test", "payload": {"id": "abc", "n": 1},
         "headers": {"source": "ha"}},
        blocking=True,
    )

    msg = await sub.next_msg(timeout=2)
    assert json.loads(msg.data) == {"id": "abc", "n": 1}
    assert msg.headers == {"source": "ha"}
    await client.close()


async def test_publish_string_payload_raw(hass, nats_server):
    client = await nats.connect(nats_server.url)
    sub = await client.subscribe("events.raw")
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)

    await hass.services.async_call(
        DOMAIN, "publish", {"subject": "events.raw", "payload": "hello"},
        blocking=True,
    )

    msg = await sub.next_msg(timeout=2)
    assert msg.data == b"hello"
    await client.close()


@pytest.mark.parametrize("subject", ["has space", "claude.*", "claude.>", "a..b", ".a"])
async def test_publish_rejects_invalid_subject(hass, nats_server, subject):
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)
    with pytest.raises((ServiceValidationError, vol.Invalid)):
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": subject, "payload": "x"}, blocking=True,
        )


async def test_publish_jetstream_returns_ack(hass, nats_server):
    client = await nats.connect(nats_server.url)
    await client.jetstream().add_stream(name="T", subjects=["claude.tasks.>"])
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)

    result = await hass.services.async_call(
        DOMAIN, "publish",
        {"subject": "claude.tasks.ask", "payload": {"q": 1}, "jetstream": True},
        blocking=True, return_response=True,
    )

    assert result == {"stream": "T", "seq": 1}
    await client.close()


async def test_subscription_fires_event_with_parsed_json(hass, nats_server):
    events = async_capture_events(hass, "nats_message")
    await _setup(hass, nats_server.url, subscribe=["claude.progress.>"])
    await _wait_connected(hass)

    client = await nats.connect(nats_server.url)
    await client.publish("claude.progress.t1", b'{"text": "working"}')
    await client.publish("claude.progress.t1", b"not json")
    await client.flush()
    await _wait_until(lambda: len(events) == 2)

    assert events[0].data["subject"] == "claude.progress.t1"
    assert events[0].data["data"] == {"text": "working"}
    assert events[1].data["data"] == "not json"
    await client.close()


async def test_durable_consumer_fires_event_and_acks(hass, nats_server):
    client = await nats.connect(nats_server.url)
    js = client.jetstream()
    await js.add_stream(name="CLAUDE_REPLIES", subjects=["claude.replies.>"])
    # Published while HA isn't connected yet: must still be delivered.
    await js.publish("claude.replies.t1", b'{"id": "t1", "status": "pr_opened"}')

    events = async_capture_events(hass, "nats_message")
    await _setup(hass, nats_server.url,
                 consume=[{"stream": "CLAUDE_REPLIES", "durable": "ha"}])
    await _wait_until(lambda: len(events) == 1)

    assert events[0].data["subject"] == "claude.replies.t1"
    assert events[0].data["data"] == {"id": "t1", "status": "pr_opened"}
    assert events[0].data["stream"] == "CLAUDE_REPLIES"

    async def acked() -> bool:
        info = await js.consumer_info("CLAUDE_REPLIES", "ha")
        return info.num_ack_pending == 0 and info.num_pending == 0

    async with asyncio.timeout(5):
        while not await acked():
            await asyncio.sleep(0.1)
    await client.close()


async def test_publish_while_disconnected_raises(hass, nats_server):
    await _setup(hass, nats_server.url)
    await _wait_connected(hass)
    nats_server.stop()
    await _wait_until(lambda: hass.states.get(SENSOR).state == "off")

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": "a.b", "payload": "x"}, blocking=True,
        )


async def test_reconnects_after_server_restart(hass, nats_server):
    events = async_capture_events(hass, "nats_message")
    await _setup(hass, nats_server.url, subscribe=["events.>"])
    await _wait_connected(hass)
    nats_server.stop()
    await _wait_until(lambda: hass.states.get(SENSOR).state == "off")
    nats_server.start()
    await _wait_connected(hass)

    client = await nats.connect(nats_server.url)
    await client.publish("events.back", b"1")
    await client.flush()
    await _wait_until(lambda: len(events) == 1, timeout=10)
    await client.close()


async def test_setup_succeeds_when_server_down_then_connects(hass, nats_server):
    nats_server.stop()
    await _setup(hass, nats_server.url)
    assert hass.states.get(SENSOR).state == "off"
    nats_server.start()
    await _wait_until(
        lambda: hass.states.get(SENSOR).state == "on", timeout=15,
    )
