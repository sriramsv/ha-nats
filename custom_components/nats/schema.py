"""Validators shared by YAML config, the config flow and the publish action."""

from __future__ import annotations

from urllib.parse import urlsplit

import voluptuous as vol

from homeassistant.helpers import config_validation as cv

from .const import CONF_DURABLE, CONF_FILTER_SUBJECT, CONF_STREAM, URL_SCHEMES


def _subject(value: str, *, wildcards: bool) -> str:
    value = cv.string(value)
    tokens = value.split(".")
    if any(not token or any(c.isspace() for c in token) for token in tokens):
        raise vol.Invalid(f"invalid NATS subject: {value!r}")
    if not wildcards and any(token in ("*", ">") for token in tokens):
        raise vol.Invalid(f"wildcards aren't allowed when publishing: {value!r}")
    if wildcards and ">" in tokens[:-1]:
        raise vol.Invalid(f"'>' must be the last token: {value!r}")
    return value


def publish_subject(value: str) -> str:
    return _subject(value, wildcards=False)


def subscribe_subject(value: str) -> str:
    return _subject(value, wildcards=True)


def nats_url(value: str) -> str:
    """A single nats:// or tls:// URL with a host."""
    value = cv.string(value).strip()
    parts = urlsplit(value)
    if parts.scheme not in URL_SCHEMES or not parts.hostname:
        raise vol.Invalid(f"expected nats://host:port or tls://host:port: {value!r}")
    return value


def url_title(url: str) -> str:
    """host:port for the entry title, without scheme or credentials."""
    parts = urlsplit(url)
    return f"{parts.hostname}:{parts.port or 4222}"


CONSUMER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_STREAM): cv.string,
        vol.Required(CONF_DURABLE): cv.string,
        vol.Optional(CONF_FILTER_SUBJECT): subscribe_subject,
    }
)

SUBSCRIBE_LIST = vol.All(cv.ensure_list, [subscribe_subject])
CONSUME_LIST = vol.All(cv.ensure_list, [CONSUMER_SCHEMA])
