# wavemq

Python SDK for the `wave-mq` custom binary protocol over TCP.

The first version is intentionally small:

- `WaveMQClient(broker, transport="tcp")`
- binary protocol over plain TCP
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
