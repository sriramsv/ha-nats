"""NATS integration: publish to NATS from HA, and turn NATS messages into events.

Configured in YAML:

    nats:
      url: nats://192.168.1.10:4222
      user: ha
      password: !secret nats_password
      subscribe:            # core subscriptions (lost while disconnected)
        - sensors.>
      consume:              # JetStream durable pull consumers (at-least-once)
        - stream: REPLIES
          durable: ha

Every inbound message fires a `nats_message` event with subject, data (parsed
JSON when possible), headers, and for consumers also stream and seq.
"""

from __future__ import annotations

import voluptuous as vol
from nats.errors import Error as NatsError

from homeassistant.const import (
    CONF_PASSWORD,
    CONF_URL,
    EVENT_HOMEASSISTANT_STOP,
    Platform,
)
from homeassistant.core import (
    Event,
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.typing import ConfigType

from .bridge import BridgeConfig, ConsumerSpec, NatsBridge, NotConnected
from .const import (
    ATTR_HEADERS,
    ATTR_JETSTREAM,
    ATTR_PAYLOAD,
    ATTR_SUBJECT,
    CONF_CLIENT_NAME,
    CONF_CONSUME,
    CONF_DURABLE,
    CONF_FILTER_SUBJECT,
    CONF_STREAM,
    CONF_SUBSCRIBE,
    DEFAULT_CLIENT_NAME,
    DOMAIN,
    SERVICE_PUBLISH,
)


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


CONSUMER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_STREAM): cv.string,
        vol.Required(CONF_DURABLE): cv.string,
        vol.Optional(CONF_FILTER_SUBJECT): subscribe_subject,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_URL): cv.string,
                vol.Optional("user"): cv.string,
                vol.Optional(CONF_PASSWORD): cv.string,
                vol.Optional(CONF_CLIENT_NAME, default=DEFAULT_CLIENT_NAME): cv.string,
                vol.Optional(CONF_SUBSCRIBE, default=[]): vol.All(
                    cv.ensure_list, [subscribe_subject]
                ),
                vol.Optional(CONF_CONSUME, default=[]): vol.All(
                    cv.ensure_list, [CONSUMER_SCHEMA]
                ),
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)

PUBLISH_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_SUBJECT): publish_subject,
        vol.Optional(ATTR_PAYLOAD): object,
        vol.Optional(ATTR_HEADERS): vol.Schema({cv.string: cv.string}),
        vol.Optional(ATTR_JETSTREAM, default=False): cv.boolean,
    }
)


def _bridge_config(conf: ConfigType) -> BridgeConfig:
    return BridgeConfig(
        url=conf[CONF_URL],
        user=conf.get("user"),
        password=conf.get(CONF_PASSWORD),
        client_name=conf[CONF_CLIENT_NAME],
        subscribe=tuple(conf[CONF_SUBSCRIBE]),
        consume=tuple(
            ConsumerSpec(
                stream=c[CONF_STREAM],
                durable=c[CONF_DURABLE],
                filter_subject=c.get(CONF_FILTER_SUBJECT),
            )
            for c in conf[CONF_CONSUME]
        ),
    )


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    bridge = NatsBridge(hass, _bridge_config(config[DOMAIN]))
    hass.data[DOMAIN] = bridge

    async def handle_publish(call: ServiceCall) -> ServiceResponse:
        try:
            ack = await bridge.publish(
                call.data[ATTR_SUBJECT],
                call.data.get(ATTR_PAYLOAD),
                call.data.get(ATTR_HEADERS),
                call.data[ATTR_JETSTREAM],
            )
        except NotConnected as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="not_connected"
            ) from err
        except (NatsError, TimeoutError) as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="publish_failed",
                translation_placeholders={
                    "subject": call.data[ATTR_SUBJECT], "error": str(err) or type(err).__name__,
                },
            ) from err
        return ack if call.return_response else None

    hass.services.async_register(
        DOMAIN, SERVICE_PUBLISH, handle_publish,
        schema=PUBLISH_SCHEMA, supports_response=SupportsResponse.OPTIONAL,
    )

    async def on_stop(_event: Event) -> None:
        await bridge.stop()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, on_stop)

    bridge.start()
    hass.async_create_task(
        async_load_platform(hass, Platform.BINARY_SENSOR, DOMAIN, {}, config)
    )
    return True
