"""Tests for the nats custom integration, against a real nats-server."""

import asyncio
import json

import nats
import pytest
import voluptuous as vol
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import async_capture_events

from .conftest import DOMAIN, setup_entry

SENSOR = "binary_sensor.nats_connected"


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


async def _wait_connected(hass: HomeAssistant) -> None:
    await _wait_until(lambda: (s := hass.states.get(SENSOR)) and s.state == "on")


def _reauth_flows(hass: HomeAssistant) -> list:
    return [
        flow for flow in hass.config_entries.flow.async_progress_by_handler(DOMAIN)
        if flow["context"]["source"] == "reauth"
    ]


async def test_connects_and_reports_connectivity(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    assert entry.state is ConfigEntryState.LOADED
    await _wait_connected(hass)
    assert hass.states.get(SENSOR).attributes["device_class"] == "connectivity"


async def test_sensor_belongs_to_a_service_device(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    entity = er.async_get(hass).async_get(SENSOR)
    assert entity.config_entry_id == entry.entry_id
    device = dr.async_get(hass).async_get(entity.device_id)
    assert device.name == "NATS"
    assert device.entry_type is dr.DeviceEntryType.SERVICE


async def test_publish_dict_payload_as_json(hass, nats_server):
    client = await nats.connect(nats_server.url)
    sub = await client.subscribe("claude.tasks.test")
    await setup_entry(hass, nats_server.url)
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
    await setup_entry(hass, nats_server.url)
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
    await setup_entry(hass, nats_server.url)
    await _wait_connected(hass)
    with pytest.raises((ServiceValidationError, vol.Invalid)):
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": subject, "payload": "x"}, blocking=True,
        )


async def test_publish_core_returns_response_when_asked(hass, nats_server):
    """Developer Tools always asks for a response; a core publish must give one."""
    await setup_entry(hass, nats_server.url)
    await _wait_connected(hass)

    result = await hass.services.async_call(
        DOMAIN, "publish", {"subject": "events.raw", "payload": "x"},
        blocking=True, return_response=True,
    )

    assert result == {"subject": "events.raw", "jetstream": False}


async def test_publish_to_denied_subject_raises(hass, nats_restricted_server):
    client = await nats.connect(
        nats_restricted_server.url, user="ha", password="secret"
    )
    sub = await client.subscribe("allowed.ok")
    await setup_entry(
        hass, nats_restricted_server.url, username="ha", password="secret"
    )
    await _wait_connected(hass)

    with pytest.raises(HomeAssistantError) as err:
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": "test", "payload": "x"}, blocking=True,
        )
    assert err.value.translation_key == "permission_denied"

    # The connection survives, and allowed subjects still work afterwards.
    await hass.services.async_call(
        DOMAIN, "publish", {"subject": "allowed.ok", "payload": "y"}, blocking=True,
    )
    assert (await sub.next_msg(timeout=2)).data == b"y"
    await client.close()


async def test_publish_jetstream_to_denied_subject_raises(hass, nats_restricted_server):
    await setup_entry(
        hass, nats_restricted_server.url, username="ha", password="secret"
    )
    await _wait_connected(hass)

    with pytest.raises(HomeAssistantError) as err:
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": "test", "payload": "x", "jetstream": True},
            blocking=True, return_response=True,
        )
    assert err.value.translation_key == "permission_denied"


async def test_publish_jetstream_returns_ack(hass, nats_server):
    client = await nats.connect(nats_server.url)
    await client.jetstream().add_stream(name="T", subjects=["claude.tasks.>"])
    await setup_entry(hass, nats_server.url)
    await _wait_connected(hass)

    result = await hass.services.async_call(
        DOMAIN, "publish",
        {"subject": "claude.tasks.ask", "payload": {"q": 1}, "jetstream": True},
        blocking=True, return_response=True,
    )

    assert result == {"subject": "claude.tasks.ask", "jetstream": True,
                      "stream": "T", "seq": 1}
    await client.close()


async def test_subscription_fires_event_with_parsed_json(hass, nats_server):
    events = async_capture_events(hass, "nats_message")
    await setup_entry(hass, nats_server.url, subscribe=["claude.progress.>"])
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
    await setup_entry(
        hass, nats_server.url, consume=[{"stream": "CLAUDE_REPLIES", "durable": "ha"}]
    )
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
    await setup_entry(hass, nats_server.url)
    await _wait_connected(hass)
    nats_server.stop()
    await _wait_until(lambda: hass.states.get(SENSOR).state == "off")

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": "a.b", "payload": "x"}, blocking=True,
        )


async def test_reconnects_after_server_restart(hass, nats_server):
    events = async_capture_events(hass, "nats_message")
    await setup_entry(hass, nats_server.url, subscribe=["events.>"])
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


async def test_setup_retries_while_server_down(hass, nats_server):
    nats_server.stop()
    entry = await setup_entry(hass, nats_server.url)
    assert entry.state is ConfigEntryState.SETUP_RETRY

    nats_server.start()
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    await _wait_connected(hass)


async def test_setup_with_wrong_password_starts_reauth(hass, nats_auth_server):
    entry = await setup_entry(
        hass, nats_auth_server.url, username="ha", password="wrong"
    )
    assert entry.state is ConfigEntryState.SETUP_ERROR
    assert len(_reauth_flows(hass)) == 1


async def test_password_rotated_while_running_starts_reauth(hass, nats_auth_server):
    await setup_entry(hass, nats_auth_server.url, username="ha", password="secret")
    await _wait_connected(hass)

    nats_auth_server.stop()
    nats_auth_server.start(password="rotated")

    await _wait_until(lambda: len(_reauth_flows(hass)) == 1, timeout=15)


async def test_unload_disconnects_and_service_reports_not_loaded(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    await _wait_connected(hass)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN, "publish", {"subject": "a.b", "payload": "x"}, blocking=True,
        )


async def test_yaml_is_imported_into_a_config_entry(hass, nats_auth_server):
    config = {
        DOMAIN: {
            "url": nats_auth_server.url,
            "user": "ha",
            "password": "secret",
            "subscribe": ["claude.progress.>"],
            "consume": [{"stream": "CLAUDE_REPLIES", "durable": "ha"}],
        }
    }
    assert await async_setup_component(hass, DOMAIN, config)
    await hass.async_block_till_done()

    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.data == {
        "url": nats_auth_server.url,
        "username": "ha",
        "password": "secret",
        "client_name": "home-assistant",
    }
    assert entry.options == {
        "subscribe": ["claude.progress.>"],
        "consume": [{"stream": "CLAUDE_REPLIES", "durable": "ha"}],
    }
    assert entry.state is ConfigEntryState.LOADED
    assert ir.async_get(hass).async_get_issue(DOMAIN, "deprecated_yaml") is not None
    await _wait_connected(hass)


async def test_yaml_import_keeps_existing_entry(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    assert await async_setup_component(
        hass, DOMAIN, {DOMAIN: {"url": "nats://elsewhere:4222"}}
    )
    await hass.async_block_till_done()

    entries = hass.config_entries.async_entries(DOMAIN)
    assert entries == [entry]
    assert entry.data["url"] == nats_server.url
