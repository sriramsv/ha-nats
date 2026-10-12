"""Diagnostics download for the NATS integration."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

from homeassistant.components.diagnostics import REDACTED, async_redact_data
from homeassistant.const import CONF_PASSWORD, CONF_URL
from homeassistant.core import HomeAssistant

from . import NatsConfigEntry

TO_REDACT = {CONF_PASSWORD}


def _redact_url(url: str) -> str:
    """Hide credentials embedded in the URL (nats://user:pass@host)."""
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit(parts._replace(netloc=f"{REDACTED}@{host}"))


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: NatsConfigEntry
) -> dict[str, Any]:
    data = async_redact_data(dict(entry.data), TO_REDACT)
    data[CONF_URL] = _redact_url(data[CONF_URL])
    return {
        "entry": {"data": data, "options": dict(entry.options)},
        "connection": entry.runtime_data.diagnostics(),
    }
