# NATS for Home Assistant

A custom integration that connects Home Assistant to a [NATS](https://nats.io)
server. It can:

- **publish** to any subject from an automation or script, optionally through
  JetStream with the stream's ack returned as the service response;
- turn messages from **core subscriptions** into `nats_message` events;
- read **JetStream durable consumers** at-least-once, so messages published
  while Home Assistant was down are delivered when it comes back;
- report the connection as `binary_sensor.nats_connected`;
- be set up entirely from the UI, with reauth, reconfigure and diagnostics.

## Installation

### HACS

1. HACS → ⋮ → **Custom repositories** → add `https://github.com/sriramsv/ha-nats`,
   category **Integration**.
2. Install **NATS** and restart Home Assistant.

### Manual

Copy `custom_components/nats` into your config's `custom_components/`
directory and restart.

## Configuration

**Settings → Devices & services → Add integration → NATS.**

1. Enter the server URL (`nats://host:4222`, or `tls://` for TLS), and a
   username and password if the server needs them. The connection is tested
   before saving: a wrong password or an unreachable server shows up in the
   form.
2. Open the integration's **Configure** to choose what to listen to:

| Option | Meaning |
|--------|---------|
| Subjects to subscribe to | Core subscriptions. Wildcards allowed (`*`, and `>` as the last token). At-most-once: messages sent while disconnected are lost. |
| JetStream durable consumers | A list of `stream`, `durable`, and optional `filter_subject`. Missing consumers are created (explicit ack, deliver all). Each message fires an event, **then** is acked, so a crash in between means redelivery, never loss. |

```yaml
# JetStream durable consumers field
- stream: REPLIES
  durable: ha
  filter_subject: replies.ha.>   # optional
```

Saving the options reconnects with the new listeners; no restart needed.
**Reconfigure** changes the URL or credentials later. If the server starts
rejecting the password (say, after a rotation), Home Assistant shows a
re-authentication prompt.

Only one NATS server per Home Assistant is supported.

If the server is unreachable when Home Assistant starts, the integration
retries setup in the background; once connected, it reconnects on its own.

### Migrating from YAML

Version 0.1 was configured in YAML. On first start with 0.2, an existing
`nats:` block is imported into the UI (the `user` key becomes the
username) and a repair notice asks you to delete the YAML. Entity IDs and
history are kept.

## Service: `nats.publish`

| Field | Required | Description |
|-------|----------|-------------|
| `subject` | yes | Subject to publish to. No spaces or wildcards. |
| `payload` | no | A mapping or list is sent as compact JSON; a string is sent as-is. |
| `headers` | no | NATS headers, string keys and values. |
| `jetstream` | no | Publish through JetStream and wait for the stream's ack. Needs a stream that captures the subject. |

With `jetstream: true` the service returns `{stream, seq}`:

```yaml
action: nats.publish
data:
  subject: tasks.ask
  payload:
    id: "{{ now().timestamp() | int }}"
    question: Why is the porch light on?
  headers:
    source: home-assistant
  jetstream: true
response_variable: ack   # → {"stream": "TASKS", "seq": 42}
```

Publishing while disconnected raises an error instead of queueing silently.

## Event: `nats_message`

Fired for every message from a subscription or consumer:

| Field | Description |
|-------|-------------|
| `subject` | The message's subject. |
| `data` | Parsed JSON when the payload is JSON, otherwise the text. |
| `headers` | Headers as a mapping, or `null`. |
| `stream`, `seq` | Only for consumer messages: the stream and its sequence number. |

```yaml
triggers:
  - trigger: event
    event_type: nats_message
    event_data:
      subject: replies.ha.done
actions:
  - action: notify.mobile_app_phone
    data:
      message: "{{ trigger.event.data.data.summary }}"
```

## Server permissions

A least-privilege user for the configuration above (publish to `tasks.>`,
consume the `REPLIES` stream with durable `ha`, subscribe to `sensors.>`):

```
ha: {
  publish: [
    "tasks.>",
    "$JS.API.CONSUMER.INFO.REPLIES.ha",
    "$JS.API.CONSUMER.CREATE.REPLIES.>",
    "$JS.API.CONSUMER.DURABLE.CREATE.REPLIES.ha",
    "$JS.API.CONSUMER.MSG.NEXT.REPLIES.ha",
    "$JS.ACK.REPLIES.ha.>"
  ]
  subscribe: ["sensors.>", "_INBOX.>"]
}
```

`_INBOX.>` is needed for JetStream acks and pull requests. Publishing with
`jetstream: true` also needs the stream to capture the subject; no extra API
permission is required.

## Logging

Connection problems are logged at `WARNING` at most, never `ERROR`, so they
don't trip automations that watch `system_log_event` for errors. Turn on
debug logging with:

```yaml
logger:
  logs:
    custom_components.nats: debug
```

## Development

Tests run against a real `nats-server` (with JetStream) started per test:

```sh
python3.14 -m venv .venv && . .venv/bin/activate
pip install -r requirements_test.txt
pytest                     # needs nats-server on PATH
```

## License

MIT
