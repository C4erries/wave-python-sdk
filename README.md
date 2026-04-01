# wavemq

Python SDK for `wave-mq` with the same client API over either the custom binary protocol or HTTP.

The first version is intentionally small:

- `WaveMQClient(broker, transport="tcp" | "http")`
- binary protocol over plain TCP or JSON HTTP API
- typed result models and protocol errors
- no external runtime dependencies

For beginner-friendly flows, the client also exposes additive helpers:

- `ensure_topic(...)`
- `produce_one(...)`
- `produce_many(...)`
- `fetch_from_offset(...)`
- `fetch_latest(...)`
- `resolve_consume_offset(...)`
- `consume_poll(...)`

## Example

```python
from wavemq import WaveMQClient

with WaveMQClient("127.0.0.1:7912") as client:
    client.ping()
    client.create_topic("demo", partitions=1, replication_factor=1)
    client.produce("demo", 0, ["hello"], key="demo-key")
    result = client.fetch("demo", 0, offset=0)
    print(result.records[0].value)
```

HTTP uses the same client surface:

```python
from wavemq import WaveMQClient

with WaveMQClient("http://127.0.0.1:8090", transport="http") as client:
    client.ping()
    client.create_topic("demo", partitions=1, replication_factor=1)
    client.produce("demo", 0, ["hello"], key="demo-key")
    result = client.fetch("demo", 0, offset=0)
    print(result.records[0].value)
```

The higher-level helpers are transport-agnostic as well and are useful for tiny producer/consumer mains and replay-style examples.

```python
from wavemq import WaveMQClient

with WaveMQClient("127.0.0.1:7912") as client:
    client.ensure_topic("demo")
    client.produce_one("demo", 0, "hello", key="demo-key")

    def handle(record):
        print(record.offset, record.value)

    start = client.resolve_consume_offset("demo-group", "demo", 0, start_from="latest")
    client.consume_poll(
        "demo-group",
        "demo",
        0,
        next_offset=start,
        max_messages=1,
        commit=True,
        on_record=handle,
    )
```
