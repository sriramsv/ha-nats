"""NATS integration: publish to NATS from HA, and turn NATS messages into events.

Set up from the UI (Settings → Devices & services → Add integration → NATS).
The connection lives in the config entry; subjects to subscribe to and
JetStream durable consumers to read are the entry's options. A legacy
`nats:` YAML block is imported into an entry once.

Every inbound message fires a `nats_message` event with subject, data (parsed
JSON when possible), headers, and for consumers also stream and seq.
"""

from __future__ import annotations

import voluptuous as vol
from nats.errors import Error as NatsError

from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import (
    CONF_PASSWORD,
    CONF_URL,
    CONF_USERNAME,
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
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    HomeAssistantError,
    ServiceValidationError,
)
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.typing import ConfigType

from .bridge import (
    BridgeConfig,
    CannotConnect,
    ConsumerSpec,
    InvalidAuth,
    NatsBridge,
    NotConnected,
    PermissionDenied,
    probe_connection,
)
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
    CONF_YAML_USER,
    DEFAULT_CLIENT_NAME,
    DOMAIN,
    ISSUE_DEPRECATED_YAML,
    SERVICE_PUBLISH,
)
from .schema import CONSUME_LIST, SUBSCRIBE_LIST, publish_subject

PLATFORMS = [Platform.BINARY_SENSOR]

type NatsConfigEntry = ConfigEntry[NatsBridge]

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_URL): cv.string,
                vol.Optional(CONF_YAML_USER): cv.string,
                vol.Optional(CONF_PASSWORD): cv.string,
                vol.Optional(CONF_CLIENT_NAME, default=DEFAULT_CLIENT_NAME): cv.string,
                vol.Optional(CONF_SUBSCRIBE, default=[]): SUBSCRIBE_LIST,
                vol.Optional(CONF_CONSUME, default=[]): CONSUME_LIST,
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


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register the publish action, and import any YAML config."""
    _register_publish(hass)
    if DOMAIN in config:
        hass.async_create_task(_import_yaml(hass, config[DOMAIN]))
    return True


async def _import_yaml(hass: HomeAssistant, conf: ConfigType) -> None:
    import_data = {
        CONF_URL: conf[CONF_URL],
        CONF_CLIENT_NAME: conf[CONF_CLIENT_NAME],
        CONF_SUBSCRIBE: conf[CONF_SUBSCRIBE],
        CONF_CONSUME: conf[CONF_CONSUME],
    }
    if CONF_YAML_USER in conf:
        import_data[CONF_USERNAME] = conf[CONF_YAML_USER]
        import_data[CONF_PASSWORD] = conf.get(CONF_PASSWORD, "")
    await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_IMPORT}, data=import_data
    )
    ir.async_create_issue(
        hass,
        DOMAIN,
        ISSUE_DEPRECATED_YAML,
        is_fixable=False,
        issue_domain=DOMAIN,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_DEPRECATED_YAML,
        translation_placeholders={"domain": DOMAIN},
    )


async def async_setup_entry(hass: HomeAssistant, entry: NatsConfigEntry) -> bool:
    config = _bridge_config(entry)
    try:
        await probe_connection(config.url, config.user, config.password, config.client_name)
    except InvalidAuth as err:
        raise ConfigEntryAuthFailed(f"{config.url} rejected the credentials") from err
    except CannotConnect as err:
        raise ConfigEntryNotReady(f"Can't reach {config.url}: {err}") from err

    bridge = NatsBridge(hass, config, on_auth_failed=lambda: entry.async_start_reauth(hass))
    entry.runtime_data = bridge
    bridge.start()

    async def on_stop(_event: Event) -> None:
        await bridge.stop()

    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, on_stop))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: NatsConfigEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.stop()
    return unloaded


def _bridge_config(entry: ConfigEntry) -> BridgeConfig:
    data, options = entry.data, entry.options
    return BridgeConfig(
        url=data[CONF_URL],
        user=data.get(CONF_USERNAME),
        password=data.get(CONF_PASSWORD),
        client_name=data.get(CONF_CLIENT_NAME, DEFAULT_CLIENT_NAME),
        subscribe=tuple(options.get(CONF_SUBSCRIBE, ())),
        consume=tuple(
            ConsumerSpec(
                stream=c[CONF_STREAM],
                durable=c[CONF_DURABLE],
                filter_subject=c.get(CONF_FILTER_SUBJECT),
            )
            for c in options.get(CONF_CONSUME, ())
        ),
    )


def _loaded_bridge(hass: HomeAssistant) -> NatsBridge:
    entries: list[NatsConfigEntry] = hass.config_entries.async_loaded_entries(DOMAIN)
    if not entries:
        raise ServiceValidationError(translation_domain=DOMAIN, translation_key="not_loaded")
    return entries[0].runtime_data


def _register_publish(hass: HomeAssistant) -> None:
    async def handle_publish(call: ServiceCall) -> ServiceResponse:
        bridge = _loaded_bridge(hass)
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
        except PermissionDenied as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="permission_denied",
                translation_placeholders={"subject": err.subject},
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
