"""Config flow for NATS: connection in the entry, listeners in the options."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlowWithReload,
)
from homeassistant.const import CONF_PASSWORD, CONF_URL, CONF_USERNAME
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    ObjectSelector,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .bridge import CannotConnect, InvalidAuth, probe_connection
from .const import (
    CONF_CLIENT_NAME,
    CONF_CONSUME,
    CONF_SUBSCRIBE,
    DEFAULT_CLIENT_NAME,
    DOMAIN,
)
from .schema import CONSUME_LIST, SUBSCRIBE_LIST, nats_url, url_title

_PASSWORD = TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD))
_URL = TextSelector(TextSelectorConfig(type=TextSelectorType.URL))


def _connection_schema(defaults: Mapping[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_URL, default=defaults.get(CONF_URL, "")): _URL,
            vol.Optional(
                CONF_USERNAME, description={"suggested_value": defaults.get(CONF_USERNAME)}
            ): str,
            vol.Optional(CONF_PASSWORD): _PASSWORD,
            vol.Optional(
                CONF_CLIENT_NAME,
                default=defaults.get(CONF_CLIENT_NAME, DEFAULT_CLIENT_NAME),
            ): str,
        }
    )


def _connection_data(user_input: Mapping[str, Any]) -> dict[str, Any]:
    """Entry data from the form; credentials only when a username is given."""
    data = {
        CONF_URL: user_input[CONF_URL].strip(),
        CONF_CLIENT_NAME: user_input.get(CONF_CLIENT_NAME) or DEFAULT_CLIENT_NAME,
    }
    if user_input.get(CONF_USERNAME):
        data[CONF_USERNAME] = user_input[CONF_USERNAME]
        data[CONF_PASSWORD] = user_input.get(CONF_PASSWORD, "")
    return data


async def _validate(data: Mapping[str, Any]) -> dict[str, str]:
    """Try the connection; return form errors (empty when it worked)."""
    try:
        nats_url(data[CONF_URL])
    except vol.Invalid:
        return {CONF_URL: "invalid_url"}
    try:
        await probe_connection(
            data[CONF_URL],
            data.get(CONF_USERNAME),
            data.get(CONF_PASSWORD),
            data[CONF_CLIENT_NAME],
        )
    except InvalidAuth:
        return {"base": "invalid_auth"}
    except CannotConnect:
        return {"base": "cannot_connect"}
    return {}


class NatsConfigFlow(ConfigFlow, domain=DOMAIN):
    """Set up the connection to one NATS server."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            data = _connection_data(user_input)
            errors = await _validate(data)
            if not errors:
                return self.async_create_entry(
                    title=url_title(data[CONF_URL]),
                    data=data,
                    options={CONF_SUBSCRIBE: [], CONF_CONSUME: []},
                )
        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(user_input or {}),
            errors=errors,
        )

    async def async_step_import(self, import_data: dict[str, Any]) -> ConfigFlowResult:
        """Move the YAML config into an entry, without a connection test.

        HA may start before the server is reachable; the entry then retries
        setup like any other.
        """
        data = {
            CONF_URL: import_data[CONF_URL],
            CONF_CLIENT_NAME: import_data.get(CONF_CLIENT_NAME, DEFAULT_CLIENT_NAME),
        }
        if import_data.get(CONF_USERNAME):
            data[CONF_USERNAME] = import_data[CONF_USERNAME]
            data[CONF_PASSWORD] = import_data.get(CONF_PASSWORD, "")
        return self.async_create_entry(
            title=url_title(data[CONF_URL]),
            data=data,
            options={
                CONF_SUBSCRIBE: list(import_data.get(CONF_SUBSCRIBE, [])),
                CONF_CONSUME: [dict(c) for c in import_data.get(CONF_CONSUME, [])],
            },
        )

    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        entry = self._get_reauth_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            data = {**entry.data, **user_input}
            errors = await _validate(data)
            if not errors:
                return self.async_update_reload_and_abort(entry, data_updates=user_input)
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_USERNAME, default=entry.data.get(CONF_USERNAME, "")
                    ): str,
                    vol.Required(CONF_PASSWORD): _PASSWORD,
                }
            ),
            description_placeholders={"url": entry.data[CONF_URL]},
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            data = _connection_data(user_input)
            errors = await _validate(data)
            if not errors:
                return self.async_update_reload_and_abort(
                    entry, title=url_title(data[CONF_URL]), data=data
                )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_connection_schema(user_input or entry.data),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> NatsOptionsFlow:
        return NatsOptionsFlow()


class NatsOptionsFlow(OptionsFlowWithReload):
    """Subjects to subscribe to and JetStream consumers to read."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            options, errors = _validate_options(user_input)
            if not errors:
                return self.async_create_entry(data=options)
        current = user_input or self.config_entry.options
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_SUBSCRIBE, default=list(current.get(CONF_SUBSCRIBE, []))
                    ): TextSelector(TextSelectorConfig(multiple=True)),
                    vol.Optional(
                        CONF_CONSUME, default=list(current.get(CONF_CONSUME, []))
                    ): ObjectSelector(),
                }
            ),
            errors=errors,
        )


def _validate_options(
    user_input: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    errors: dict[str, str] = {}
    subscribe: list[str] = []
    consume: list[dict[str, Any]] = []
    try:
        subscribe = SUBSCRIBE_LIST(user_input.get(CONF_SUBSCRIBE) or [])
    except vol.Invalid:
        errors[CONF_SUBSCRIBE] = "invalid_subject"
    try:
        consume = [dict(c) for c in CONSUME_LIST(user_input.get(CONF_CONSUME) or [])]
    except vol.Invalid:
        errors[CONF_CONSUME] = "invalid_consumer"
    return {CONF_SUBSCRIBE: subscribe, CONF_CONSUME: consume}, errors
