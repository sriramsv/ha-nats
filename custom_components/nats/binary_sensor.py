"""Connectivity sensor for the NATS connection."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import NatsConfigEntry
from .bridge import NatsBridge
from .const import DOMAIN, SIGNAL_CONNECTION_STATE


async def async_setup_entry(
    hass: HomeAssistant,
    entry: NatsConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([NatsConnectedSensor(entry.runtime_data, entry.entry_id)])


class NatsConnectedSensor(BinarySensorEntity):
    """On while the NATS connection is up."""

    _attr_has_entity_name = True
    _attr_translation_key = "connected"
    # Same unique_id as the YAML-era entity, so its entity_id and history carry over.
    _attr_unique_id = "nats_connected"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_should_poll = False

    def __init__(self, bridge: NatsBridge, entry_id: str) -> None:
        self._bridge = bridge
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name="NATS",
            manufacturer="NATS",
            entry_type=DeviceEntryType.SERVICE,
        )

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
