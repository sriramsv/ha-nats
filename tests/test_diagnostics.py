"""Tests for the nats diagnostics download."""

from pytest_homeassistant_custom_component.components.diagnostics import (
    get_diagnostics_for_config_entry,
)

from .conftest import setup_entry


async def test_diagnostics_redacts_password(hass, hass_client, nats_auth_server):
    entry = await setup_entry(
        hass, nats_auth_server.url, username="ha", password="secret",
        subscribe=["events.>"],
    )

    diag = await get_diagnostics_for_config_entry(hass, hass_client, entry)

    assert diag["entry"]["data"]["password"] == "**REDACTED**"
    assert diag["entry"]["data"]["username"] == "ha"
    assert diag["entry"]["options"]["subscribe"] == ["events.>"]
    assert "connected" in diag["connection"]
    assert "secret" not in str(diag)


async def test_diagnostics_strips_credentials_from_url(hass, hass_client, nats_auth_server):
    url = f"nats://ha:secret@127.0.0.1:{nats_auth_server.port}"
    entry = await setup_entry(hass, url)

    diag = await get_diagnostics_for_config_entry(hass, hass_client, entry)

    assert diag["entry"]["data"]["url"] == f"nats://**REDACTED**@127.0.0.1:{nats_auth_server.port}"
    assert "secret" not in str(diag)
