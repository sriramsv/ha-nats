"""Tests for the nats config flow, options flow, reauth and reconfigure."""

import asyncio

import nats
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import async_capture_events

from .conftest import DOMAIN, _free_port, setup_entry


async def _start_user_flow(hass: HomeAssistant) -> dict:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    return result


async def test_user_flow_creates_entry(hass, nats_server):
    result = await _start_user_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"url": nats_server.url}
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == f"127.0.0.1:{nats_server.port}"
    assert result["data"] == {"url": nats_server.url, "client_name": "home-assistant"}
    assert result["options"] == {"subscribe": [], "consume": []}


async def test_user_flow_with_credentials(hass, nats_auth_server):
    result = await _start_user_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {"url": nats_auth_server.url, "username": "ha", "password": "secret",
         "client_name": "ha-test"},
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        "url": nats_auth_server.url, "username": "ha", "password": "secret",
        "client_name": "ha-test",
    }


async def test_user_flow_invalid_auth(hass, nats_auth_server):
    result = await _start_user_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {"url": nats_auth_server.url, "username": "ha", "password": "wrong"},
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}

    # Recovers once the right password is entered.
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {"url": nats_auth_server.url, "username": "ha", "password": "secret"},
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY


async def test_user_flow_cannot_connect(hass, socket_enabled):
    result = await _start_user_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"url": f"nats://127.0.0.1:{_free_port()}"}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_user_flow_rejects_non_nats_url(hass):
    result = await _start_user_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"url": "http://127.0.0.1:4222"}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"url": "invalid_url"}


async def test_only_one_entry_allowed(hass, nats_server):
    await setup_entry(hass, nats_server.url)
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "single_instance_allowed"


async def test_reauth_updates_password_and_reloads(hass, nats_auth_server):
    entry = await setup_entry(
        hass, nats_auth_server.url, username="ha", password="wrong"
    )
    result = await entry.start_reauth_flow(hass)
    assert result["step_id"] == "reauth_confirm"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"username": "ha", "password": "nope"}
    )
    assert result["errors"] == {"base": "invalid_auth"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"username": "ha", "password": "secret"}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data["password"] == "secret"
    await hass.async_block_till_done()
    assert entry.state is config_entries.ConfigEntryState.LOADED


async def test_reconfigure_changes_url(hass, nats_server, nats_auth_server):
    entry = await setup_entry(hass, nats_server.url)
    result = await entry.start_reconfigure_flow(hass)
    assert result["step_id"] == "reconfigure"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {"url": nats_auth_server.url, "username": "ha", "password": "secret"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data["url"] == nats_auth_server.url
    assert entry.data["username"] == "ha"
    assert entry.title == f"127.0.0.1:{nats_auth_server.port}"


async def test_reconfigure_cannot_connect(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    result = await entry.start_reconfigure_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"url": f"nats://127.0.0.1:{_free_port()}"}
    )

    assert result["errors"] == {"base": "cannot_connect"}
    assert entry.data["url"] == nats_server.url


async def test_options_flow_updates_subscriptions_and_reloads(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    events = async_capture_events(hass, "nats_message")

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {"subscribe": ["events.>"],
         "consume": [{"stream": "S", "durable": "d", "filter_subject": "s.>"}]},
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        "subscribe": ["events.>"],
        "consume": [{"stream": "S", "durable": "d", "filter_subject": "s.>"}],
    }
    await hass.async_block_till_done()

    client = await nats.connect(nats_server.url)
    async with asyncio.timeout(5):
        while not events:
            await client.publish("events.x", b"1")
            await client.flush()
            await asyncio.sleep(0.1)
    await client.close()


async def test_options_flow_rejects_bad_subject(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"subscribe": ["bad subject"], "consume": []}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"subscribe": "invalid_subject"}


async def test_options_flow_rejects_bad_consumer(hass, nats_server):
    entry = await setup_entry(hass, nats_server.url)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"subscribe": [], "consume": [{"stream": "S"}]}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"consume": "invalid_consumer"}
