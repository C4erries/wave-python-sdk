from __future__ import annotations

import json
import socket
import socketserver
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from wavemq import WaveMQClient  # noqa: E402
from wavemq.models import Header, PartitionMetadata, PartitionRole, Record  # noqa: E402
from wavemq.protocol import (  # noqa: E402
    API_KEY_COMMIT_OFFSET,
    API_KEY_CREATE_TOPIC,
    API_KEY_FETCH,
    API_KEY_FETCH_COMMITTED,
    API_KEY_LIST_OFFSETS,
    API_KEY_METADATA,
    API_KEY_PING,
    API_KEY_PRODUCE,
    decode_commit_offset_request,
    decode_create_topic_request,
    decode_fetch_committed_request,
    decode_fetch_request,
    decode_list_offsets_request,
    decode_metadata_request,
    decode_produce_request,
    encode_commit_offset_response,
    encode_create_topic_response,
    encode_fetch_committed_response,
    encode_fetch_response,
    encode_list_offsets_response,
    encode_metadata_response,
    encode_ping_response,
    encode_produce_response,
    encode_response_frame,
    encode_frame,
    decode_request_frame,
)


class FakeWaveMQServer:
    def __init__(self, handler):
        self._handler = handler
        self._stop = threading.Event()
        self._ready = threading.Event()
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self._addr = self._sock.getsockname()
        self._thread.start()
        self._ready.wait(5)

    @property
    def broker(self) -> str:
        host, port = self._addr
        return f"{host}:{port}"

    def close(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        self._thread.join(timeout=5)

    def assert_ok(self) -> None:
        if self.error is not None:
            raise self.error

    def _serve(self) -> None:
        self._ready.set()
        try:
            conn, _ = self._sock.accept()
        except OSError:
            return
        with conn:
            reader = conn.makefile("rb")
            while not self._stop.is_set():
                try:
                    api_key, correlation_id, payload = decode_request_frame(reader)
                except Exception:
                    break
                try:
                    response_api_key, response_payload = self._handler(api_key, payload)
                    conn.sendall(encode_response_frame(response_api_key, correlation_id, response_payload))
                except BaseException as exc:  # pragma: no cover - stored for assertion
                    self.error = exc
                    break


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class FakeWaveMQHTTPServer:
    def __init__(self) -> None:
        self.state = {
            "messages": [],
            "next_offset": 3,
            "metadata_calls": 0,
            "committed_offset": 4,
        }
        handler = self._make_handler()
        self._server = _ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def broker(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    self._write_text(200, "ok")
                    return
                if parsed.path == "/api/topics":
                    self._write_json(
                        200,
                        [
                            {"name": "demo", "partitions": 1, "replicationFactor": 1},
                        ],
                    )
                    return
                if parsed.path == "/api/topics/demo":
                    outer.state["metadata_calls"] += 1
                    self._write_json(200, self._topic_detail())
                    return
                if parsed.path == "/api/topics/demo/partitions/0/messages":
                    qs = parse_qs(parsed.query)
                    target = int(qs.get("offset", ["0"])[0])
                    limit = int(qs.get("limit", ["50"])[0])
                    records = [item for item in outer.state["messages"] if item["offset"] <= target]
                    if len(records) > limit:
                        records = records[-limit:]
                    self._write_json(200, list(reversed(records)))
                    return
                if parsed.path == "/api/consumers/g/topics/demo/partitions/0/offset":
                    self._write_json(
                        200,
                        {
                            "group": "g",
                            "topic": "demo",
                            "partition": 0,
                            "offset": outer.state["committed_offset"],
                        },
                    )
                    return
                self.send_error(404)

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length else b""
                data = json.loads(body.decode("utf-8") or "{}")
                if parsed.path == "/api/topics":
                    self._write_json(200, self._topic_detail())
                    return
                if parsed.path == "/api/topics/demo/partitions/0/messages":
                    offset = outer.state["next_offset"]
                    outer.state["next_offset"] += 1
                    outer.state["messages"].append(
                        {
                            "partition": 0,
                            "offset": offset,
                            "key": data.get("key"),
                            "value": data.get("value"),
                            "timestamp": "2026-03-18T10:00:00Z",
                        }
                    )
                    self._write_json(200, {"partition": 0, "baseOffset": offset})
                    return
                if parsed.path == "/api/consumers/g/topics/demo/partitions/0/offset":
                    outer.state["committed_offset"] = int(data["offset"])
                    self._write_json(
                        200,
                        {
                            "group": "g",
                            "topic": "demo",
                            "partition": 0,
                            "offset": outer.state["committed_offset"],
                        },
                    )
                    return
                self.send_error(404)

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

            def _topic_detail(self) -> dict[str, object]:
                return {
                    "name": "demo",
                    "partitionCount": 1,
                    "replicationFactor": 1,
                    "partitions": [
                        {
                            "id": 0,
                            "leader": 1,
                            "role": "leader",
                            "highWatermark": outer.state["next_offset"] - 1,
                            "startOffset": 0,
                            "replicas": [1],
                            "isr": [1],
                            "leaderEpoch": 1,
                        }
                    ],
                }

            def _write_json(self, status: int, payload: object) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def _write_text(self, status: int, payload: str) -> None:
                encoded = payload.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        return Handler


class ClientTests(unittest.TestCase):
    def test_transport_selection(self) -> None:
        client = WaveMQClient("127.0.0.1:1")
        self.assertEqual("tcp", client.transport)
        client.close()
        http_client = WaveMQClient("127.0.0.1:8090", transport="http")
        self.assertEqual("http", http_client.transport)
        http_client.close()
        with self.assertRaises(ValueError):
            WaveMQClient("127.0.0.1:1", transport="ftp")

    def test_tcp_roundtrip_and_cache(self) -> None:
        metadata_calls = {"count": 0}

        def handler(api_key, payload):
            if api_key == API_KEY_PING:
                self.assertEqual(b"", payload)
                return api_key, encode_ping_response(0)
            if api_key == API_KEY_CREATE_TOPIC:
                self.assertEqual(("demo", 1, 1), decode_create_topic_request(payload))
                return api_key, encode_create_topic_response(0)
            if api_key == API_KEY_PRODUCE:
                topic, partition, records = decode_produce_request(payload)
                self.assertEqual("demo", topic)
                self.assertEqual(0, partition)
                self.assertEqual(1, len(records))
                self.assertEqual(b"demo-key", records[0].key)
                self.assertEqual(b"hello", records[0].value)
                return api_key, encode_produce_response(3, 0)
            if api_key == API_KEY_FETCH:
                self.assertEqual(("demo", 0, 0, 1_048_576), decode_fetch_request(payload))
                records = (
                    Record(offset=3, value=b"hello"),
                    Record(offset=4, value=b"world"),
                )
                return api_key, encode_fetch_response(records, high_watermark=5, error=0)
            if api_key == API_KEY_METADATA:
                metadata_calls["count"] += 1
                self.assertEqual(("demo",), decode_metadata_request(payload))
                partition = PartitionMetadata(
                    topic="demo",
                    partition=0,
                    broker_id=1,
                    role=PartitionRole.LEADER,
                    leader_epoch=1,
                    start_offset=0,
                    high_watermark=5,
                    leader=1,
                    replicas=(1,),
                    isr=(1,),
                )
                return api_key, encode_metadata_response((partition,), 0)
            if api_key == API_KEY_LIST_OFFSETS:
                self.assertEqual(("demo", 0), decode_list_offsets_request(payload))
                return api_key, encode_list_offsets_response(0, 5, 0)
            if api_key == API_KEY_COMMIT_OFFSET:
                self.assertEqual(("g", "demo", 0, 4), decode_commit_offset_request(payload))
                return api_key, encode_commit_offset_response(0)
            if api_key == API_KEY_FETCH_COMMITTED:
                self.assertEqual(("g", "demo", 0), decode_fetch_committed_request(payload))
                return api_key, encode_fetch_committed_response(4, 0)
            raise AssertionError(f"unexpected api key {api_key}")

        server = FakeWaveMQServer(handler)
        try:
            with WaveMQClient(server.broker, metadata_ttl=60.0) as client:
                self.assertEqual("tcp", client.transport)
                self.assertGreaterEqual(client.ping().rtt_ms, 0.0)
                self.assertEqual(
                    "demo",
                    client.create_topic("demo", partitions=1, replication_factor=1).topic,
                )
                self.assertEqual(
                    3,
                    client.produce("demo", 0, ["hello"], key="demo-key").base_offset,
                )
                fetched = client.fetch("demo", 0, 0)
                self.assertEqual(2, len(fetched.records))
                self.assertEqual(b"hello", fetched.records[0].value)
                self.assertEqual(5, fetched.high_watermark)
                metadata = client.metadata("demo")
                self.assertEqual(1, len(metadata.partitions))
                self.assertEqual(1, metadata.partitions[0].broker_id)
                self.assertEqual(0, client.list_offsets("demo", 0).earliest)
                self.assertEqual(5, client.list_offsets("demo", 0).latest)
                self.assertEqual(4, client.commit_offset("g", "demo", 0, 4).offset)
                self.assertEqual(4, client.fetch_committed("g", "demo", 0).offset)
                self.assertEqual(1, metadata_calls["count"])
                self.assertEqual(1, len(client.metadata("demo").partitions))
                self.assertEqual(1, metadata_calls["count"])
        finally:
            server.close()
            server.assert_ok()

    def test_http_roundtrip_and_cache(self) -> None:
        server = FakeWaveMQHTTPServer()
        try:
            with WaveMQClient(server.broker, transport="http", metadata_ttl=60.0) as client:
                self.assertEqual("http", client.transport)
                self.assertGreaterEqual(client.ping().rtt_ms, 0.0)
                created = client.create_topic("demo", partitions=1, replication_factor=1)
                self.assertEqual("demo", created.topic)
                produced = client.produce("demo", 0, ["hello", "world"], key="demo-key")
                self.assertEqual(3, produced.base_offset)
                fetched = client.fetch("demo", 0, offset=3)
                self.assertEqual(2, len(fetched.records))
                self.assertEqual(b"hello", fetched.records[0].value)
                self.assertEqual(4, fetched.high_watermark)
                metadata = client.metadata("demo")
                self.assertEqual(1, len(metadata.partitions))
                self.assertEqual(1, metadata.partitions[0].broker_id)
                metadata_calls = server.state["metadata_calls"]
                self.assertEqual(1, len(client.metadata("demo").partitions))
                self.assertEqual(metadata_calls, server.state["metadata_calls"])
                self.assertEqual(0, client.list_offsets("demo", 0).earliest)
                self.assertEqual(4, client.list_offsets("demo", 0).latest)
                self.assertEqual(5, client.commit_offset("g", "demo", 0, 5).offset)
                self.assertEqual(5, client.fetch_committed("g", "demo", 0).offset)
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
