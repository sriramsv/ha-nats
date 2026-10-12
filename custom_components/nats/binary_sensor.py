"""Connectivity sensor for the NATS connection."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from .bridge import NatsBridge
from .const import DOMAIN, SIGNAL_CONNECTION_STATE


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    if discovery_info is None:
        return
    async_add_entities([NatsConnectedSensor(hass.data[DOMAIN])])


class NatsConnectedSensor(BinarySensorEntity):
    """On while the NATS connection is up."""

    _attr_name = "NATS connected"
    _attr_unique_id = "nats_connected"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_should_poll = False

    def __init__(self, bridge: NatsBridge) -> None:
        self._bridge = bridge

    @property
    def is_on(self) -> bool:
        return self._bridge.connected

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, SIGNAL_CONNECTION_STATE, self._handle_state
            )
        )

    @callback
    def _handle_state(self, _connected: bool) -> None:
        self.async_write_ha_state()
