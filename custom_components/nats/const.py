"""Constants for the NATS integration."""

DOMAIN = "nats"

CONF_SUBSCRIBE = "subscribe"
CONF_CONSUME = "consume"
CONF_STREAM = "stream"
CONF_DURABLE = "durable"
CONF_FILTER_SUBJECT = "filter_subject"
CONF_CLIENT_NAME = "client_name"

DEFAULT_CLIENT_NAME = "home-assistant"

EVENT_NATS_MESSAGE = "nats_message"
SIGNAL_CONNECTION_STATE = f"{DOMAIN}_connection_state"

SERVICE_PUBLISH = "publish"
ATTR_SUBJECT = "subject"
ATTR_PAYLOAD = "payload"
ATTR_HEADERS = "headers"
ATTR_JETSTREAM = "jetstream"

# Seconds between attempts while the first connection can't be made.
# After that, nats-py's own reconnect loop takes over.
INITIAL_CONNECT_RETRY = 5
RECONNECT_WAIT = 2
CONNECT_TIMEOUT = 5
FETCH_BATCH = 10
FETCH_TIMEOUT = 5
