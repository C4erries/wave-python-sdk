# wavemq

`wavemq` is the Python SDK for `wave-mq`. It provides one client API over two transports:

- `transport="tcp"` for the custom binary protocol
- `transport="http"` for the broker HTTP API

TCP is the default and is the best fit for binary payloads. HTTP is useful for preview flows and simpler integrations.

## Install

```powershell
python -m pip install wave-python-sdk
```

## Quick start

```python
from wavemq import WaveMQClient

with WaveMQClient("127.0.0.1:7912") as client:
    client.ping()
    client.create_topic("demo", partitions=1, replication_factor=1)
    client.produce("demo", 0, ["hello"], key="demo-key")
    result = client.fetch("demo", 0, offset=0)
    print(result.records[0].value)
```

HTTP uses the same surface:

```python
from wavemq import WaveMQClient

with WaveMQClient("http://127.0.0.1:8090", transport="http") as client:
    client.ping()
    client.create_topic("demo", partitions=1, replication_factor=1)
    client.produce("demo", 0, ["hello"], key="demo-key")
    result = client.fetch("demo", 0, offset=0)
    print(result.records[0].value)
```

## Helper API

For tiny scripts and preview examples, the client also exposes additive helpers:

- `ensure_topic(...)`
- `produce_one(...)`
- `produce_many(...)`
- `fetch_from_offset(...)`
- `fetch_latest(...)`
- `resolve_consume_offset(...)`
- `consume_poll(...)`

`Record` also supports `content_type`, which maps to the canonical `content-type` header on TCP and to the `contentType` JSON field on HTTP.

By default, consumer helpers resume from `committed + 1` when a group already has state.
If the group has no committed offset yet, they start from the earliest available offset.

## Examples

Runnable examples live in:

- `examples/python/sdk/tcp`
- `examples/python/sdk/http`

The TCP folder also contains `simple` in-file producer/consumer/replay scripts for quick manual checks and offset reruns.

## Notes

- Use `transport="tcp"` if you need raw bytes and the binary protocol.
- Use `transport="http"` if you want a broker-address-only integration path.
- The package is intentionally small and has no runtime dependencies beyond the standard library.
