# wavemq

Python SDK for `wave-mq` with the same client API over either the custom binary protocol or HTTP.

The first version is intentionally small:

- `WaveMQClient(broker, transport="tcp" | "http")`
- binary protocol over plain TCP or JSON HTTP API
- typed result models and protocol errors
- no HTTP and no external runtime dependencies

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
