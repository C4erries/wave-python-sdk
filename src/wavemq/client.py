from __future__ import annotations

import base64
import json
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .errors import (
    UnsupportedFeatureError,
    TopicExistsError,
    WaveMQBrokerError,
    WaveMQConnectionError,
    WaveMQProtocolError,
    broker_exception_for_error_code,
    broker_exception_for_http,
)
from .models import (
    CommitOffsetResult,
    ConsumePollResult,
    CreateTopicResult,
    EnsureTopicResult,
    FetchCommittedResult,
    FetchResult,
    ListOffsetsResult,
    MetadataResult,
    PartitionMetadata,
    PartitionRole,
    PingResult,
    ProduceResult,
    Record,
)
from .protocol import (
    API_KEY_COMMIT_OFFSET,
    API_KEY_CREATE_TOPIC,
    API_KEY_FETCH,
    API_KEY_FETCH_COMMITTED,
    API_KEY_LIST_OFFSETS,
    API_KEY_METADATA,
    API_KEY_PING,
    API_KEY_PRODUCE,
    decode_commit_offset_response,
    decode_create_topic_response,
    decode_fetch_committed_response,
    decode_fetch_response,
    decode_frame,
    decode_list_offsets_response,
    decode_metadata_response,
    decode_ping_response,
    decode_produce_response,
    encode_commit_offset_request,
    encode_create_topic_request,
    encode_fetch_committed_request,
    encode_fetch_request,
    encode_frame,
    encode_list_offsets_request,
    encode_metadata_request,
    encode_ping_request,
    encode_produce_request,
)
from .routing import MetadataCache, normalize_topics


class _TcpTransport:
    def __init__(self, broker: str, timeout: float) -> None:
        host, port = _parse_broker(broker)
        self._broker = f"{host}:{port}"
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock: socket.socket | None = None
        self._reader = None
        self._lock = threading.Lock()
        self._corr = 0

    @property
    def broker(self) -> str:
        return self._broker

    def close(self) -> None:
        reader = self._reader
        self._reader = None
        sock = self._sock
        self._sock = None
        if reader is not None:
            try:
                reader.close()
            except OSError:
                pass
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def request(self, api_key: int, payload: bytes) -> bytes:
        with self._lock:
            sock = self._ensure_socket()
            self._corr += 1
            correlation_id = self._corr
            frame = encode_frame(api_key, correlation_id, payload)
            try:
                sock.sendall(frame)
                resp_api_key, resp_corr, resp_payload = decode_frame(self._reader)
            except OSError as exc:
                self.close()
                raise WaveMQConnectionError("tcp request failed", broker=self._broker) from exc

            if resp_corr != correlation_id:
                raise WaveMQProtocolError(
                    f"correlation mismatch expected {correlation_id} got {resp_corr}",
                    broker=self._broker,
                )
            if resp_api_key != api_key:
                raise WaveMQProtocolError(
                    f"unexpected response api key {resp_api_key} for request {api_key}",
                    broker=self._broker,
                )
            return resp_payload

    def _ensure_socket(self) -> socket.socket:
        if self._sock is not None and self._reader is not None:
            return self._sock
        try:
            sock = socket.create_connection((self._host, self._port), timeout=self._timeout)
        except OSError as exc:
            raise WaveMQConnectionError("failed to connect", broker=self._broker) from exc
        sock.settimeout(self._timeout)
        self._sock = sock
        self._reader = sock.makefile("rb")
        return sock


class _HttpTransport:
    def __init__(self, broker: str, timeout: float) -> None:
        self._base_url = _normalize_http_broker(broker)
        self._timeout = timeout

    @property
    def broker(self) -> str:
        return self._base_url

    def close(self) -> None:
        return None

    def ping(self) -> PingResult:
        started = time.perf_counter()
        self._request_text("GET", "/healthz")
        return PingResult(rtt_ms=(time.perf_counter() - started) * 1000.0)

    def create_topic(self, topic: str, partitions: int, replication_factor: int) -> CreateTopicResult:
        payload = {
            "name": topic,
            "partitions": partitions,
            "replicationFactor": replication_factor,
        }
        try:
            self._request_json("POST", "/api/topics", payload=payload)
        except HTTPError as exc:
            raise _http_error(exc, broker=self.broker, action=f"create-topic {topic}") from exc
        return CreateTopicResult(topic=topic, partitions=partitions, replication_factor=replication_factor)

    def produce(self, topic: str, partition: int, records: Sequence[Record]) -> ProduceResult:
        if not records:
            raise ValueError("records must not be empty")

        base_offset: int | None = None
        path = f"/api/topics/{quote(topic, safe='')}/partitions/{partition}/messages"
        for record in records:
            if record.headers:
                raise UnsupportedFeatureError(
                    "http transport does not support record headers",
                    broker=self.broker,
                )
            payload: dict[str, Any] = {"value": _encode_record_bytes(record.value)}
            if record.key is not None:
                payload["key"] = _encode_record_bytes(record.key)
            try:
                data = self._request_json("POST", path, payload=payload)
            except HTTPError as exc:
                raise _http_error(exc, broker=self.broker, action=f"produce {topic}/{partition}") from exc
            if base_offset is None:
                base_offset = int(data["baseOffset"])
        return ProduceResult(base_offset=0 if base_offset is None else base_offset)

    def fetch(self, topic: str, partition: int, offset: int, max_bytes: int = 1_048_576) -> FetchResult:
        _ = max_bytes
        partition_detail = self._partition_detail(topic, partition)
        high_watermark = partition_detail.high_watermark
        if high_watermark < offset:
            return FetchResult(records=(), high_watermark=high_watermark)
        limit = max(1, high_watermark - offset + 1)
        path = (
            f"/api/topics/{quote(topic, safe='')}/partitions/{partition}/messages"
            f"?offset={high_watermark}&limit={limit}"
        )
        try:
            data = self._request_json("GET", path)
        except HTTPError as exc:
            raise _http_error(exc, broker=self.broker, action=f"fetch {topic}/{partition}") from exc
        if not isinstance(data, list):
            raise WaveMQProtocolError("expected list response for fetch", broker=self.broker)
        records = tuple(
            item
            for item in sorted((_record_from_http(item) for item in data), key=lambda item: item.offset)
            if item.offset >= offset
        )
        return FetchResult(records=records, high_watermark=high_watermark)

    def metadata(self, topics: tuple[str, ...]) -> MetadataResult:
        requested = topics
        if not requested:
            try:
                summaries = self._request_json("GET", "/api/topics")
            except HTTPError as exc:
                raise _http_error(exc, broker=self.broker, action="metadata") from exc
            if not isinstance(summaries, list):
                raise WaveMQProtocolError("expected list response for topics", broker=self.broker)
            requested = tuple(str(item["name"]) for item in summaries if isinstance(item, dict) and "name" in item)

        partitions: list[PartitionMetadata] = []
        for topic in requested:
            partitions.extend(self._topic_metadata(topic))
        return MetadataResult(partitions=tuple(partitions))

    def list_offsets(self, topic: str, partition: int) -> ListOffsetsResult:
        detail = self._partition_detail(topic, partition)
        return ListOffsetsResult(earliest=detail.start_offset, latest=detail.high_watermark)

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> CommitOffsetResult:
        path = f"/api/consumers/{quote(group, safe='')}/topics/{quote(topic, safe='')}/partitions/{partition}/offset"
        try:
            self._request_json("POST", path, payload={"offset": offset})
        except HTTPError as exc:
            raise _http_error(exc, broker=self.broker, action=f"commit-offset {group} {topic}/{partition}") from exc
        return CommitOffsetResult(group=group, topic=topic, partition=partition, offset=offset)

    def fetch_committed(self, group: str, topic: str, partition: int) -> FetchCommittedResult:
        path = f"/api/consumers/{quote(group, safe='')}/topics/{quote(topic, safe='')}/partitions/{partition}/offset"
        try:
            data = self._request_json("GET", path)
        except HTTPError as exc:
            raise _http_error(exc, broker=self.broker, action=f"fetch-committed {group} {topic}/{partition}") from exc
        return FetchCommittedResult(offset=int(data["offset"]))

    def _topic_metadata(self, topic: str) -> tuple[PartitionMetadata, ...]:
        path = f"/api/topics/{quote(topic, safe='')}"
        try:
            data = self._request_json("GET", path)
        except HTTPError as exc:
            raise _http_error(exc, broker=self.broker, action=f"metadata {topic}") from exc
        if not isinstance(data, dict):
            raise WaveMQProtocolError("expected object response for topic detail", broker=self.broker)
        partitions = data.get("partitions")
        if not isinstance(partitions, list):
            raise WaveMQProtocolError("topic detail missing partitions", broker=self.broker)
        result: list[PartitionMetadata] = []
        for item in partitions:
            if not isinstance(item, dict):
                continue
            result.append(_partition_metadata_from_http(topic, item))
        return tuple(result)

    def _partition_detail(self, topic: str, partition: int) -> PartitionMetadata:
        for item in self._topic_metadata(topic):
            if item.partition == partition:
                return item
        raise broker_exception_for_http(
            404,
            f"partition {partition} not found",
            broker=self.broker,
        )

    def _request_text(self, method: str, path: str, payload: dict[str, Any] | None = None) -> str:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json, text/plain;q=0.9"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = Request(self._base_url + path, data=data, headers=headers, method=method)
        try:
            with urlopen(req, timeout=self._timeout) as response:
                raw = response.read()
        except HTTPError:
            raise
        except URLError as exc:
            raise WaveMQConnectionError("http request failed", broker=self.broker) from exc
        return raw.decode("utf-8", errors="replace")

    def _request_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        text = self._request_text(method, path, payload=payload)
        if not text.strip():
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise WaveMQProtocolError("expected json response", broker=self.broker) from exc


class WaveMQClient:
    def __init__(
        self,
        broker: str,
        timeout: float = 10.0,
        auto_route: bool = True,
        metadata_ttl: float = 30.0,
        *,
        transport: str = "tcp",
    ) -> None:
        transport = transport.lower().strip()
        if transport not in {"tcp", "http"}:
            raise ValueError(f"unsupported transport {transport!r}")

        self._broker = broker
        self._timeout = float(timeout)
        self._auto_route = bool(auto_route)
        self._metadata_cache = MetadataCache(metadata_ttl)
        self._transport_name = transport
        if transport == "tcp":
            self._transport = _TcpTransport(broker, timeout=self._timeout)
        else:
            self._transport = _HttpTransport(broker, timeout=self._timeout)

    @property
    def broker(self) -> str:
        return self._broker

    @property
    def transport(self) -> str:
        return self._transport_name

    @property
    def auto_route(self) -> bool:
        return self._auto_route

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "WaveMQClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ping(self) -> PingResult:
        if self._transport_name == "http":
            return self._transport.ping()

        started = time.perf_counter()
        payload = encode_ping_request()
        response = decode_ping_response(self._transport.request(API_KEY_PING, payload))
        self._raise_for_error(response.error, "ping")
        return PingResult(rtt_ms=(time.perf_counter() - started) * 1000.0)

    def create_topic(self, topic: str, partitions: int = 1, replication_factor: int = 1) -> CreateTopicResult:
        if self._transport_name == "http":
            return self._transport.create_topic(topic, partitions, replication_factor)

        response = decode_create_topic_response(
            self._transport.request(
                API_KEY_CREATE_TOPIC,
                encode_create_topic_request(topic, partitions, replication_factor),
            )
        )
        self._raise_for_error(response.error, f"create-topic {topic}")
        return CreateTopicResult(topic=topic, partitions=partitions, replication_factor=replication_factor)

    def ensure_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
    ) -> EnsureTopicResult:
        try:
            created = self.create_topic(topic, partitions=partitions, replication_factor=replication_factor)
            return EnsureTopicResult(
                topic=created.topic,
                partitions=created.partitions,
                replication_factor=created.replication_factor,
                created=True,
            )
        except TopicExistsError:
            return EnsureTopicResult(
                topic=topic,
                partitions=partitions,
                replication_factor=replication_factor,
                created=False,
            )

    def produce(
        self,
        topic: str,
        partition: int,
        records: Sequence[Record | bytes | str],
        key: bytes | str | None = None,
    ) -> ProduceResult:
        encoded_records = _coerce_records(records, key=key)
        if self._transport_name == "http":
            return self._transport.produce(topic, partition, encoded_records)

        response = decode_produce_response(
            self._transport.request(API_KEY_PRODUCE, encode_produce_request(topic, partition, encoded_records))
        )
        self._raise_for_error(response.error, f"produce {topic}/{partition}")
        return ProduceResult(base_offset=response.base_offset)

    def produce_one(
        self,
        topic: str,
        partition: int,
        value: Record | bytes | str,
        key: bytes | str | None = None,
    ) -> ProduceResult:
        return self.produce(topic, partition, [value], key=key)

    def produce_many(
        self,
        topic: str,
        partition: int,
        values: Sequence[Record | bytes | str],
        key: bytes | str | None = None,
    ) -> ProduceResult:
        return self.produce(topic, partition, values, key=key)

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_bytes: int = 1_048_576,
    ) -> FetchResult:
        if self._transport_name == "http":
            return self._transport.fetch(topic, partition, offset, max_bytes=max_bytes)

        response = decode_fetch_response(
            self._transport.request(API_KEY_FETCH, encode_fetch_request(topic, partition, offset, max_bytes))
        )
        self._raise_for_error(response.error, f"fetch {topic}/{partition}")
        return FetchResult(records=response.records, high_watermark=response.high_watermark)

    def fetch_from_offset(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_bytes: int = 1_048_576,
    ) -> FetchResult:
        return self.fetch(topic, partition, offset, max_bytes=max_bytes)

    def fetch_latest(
        self,
        topic: str,
        partition: int,
        max_bytes: int = 1_048_576,
    ) -> FetchResult:
        offsets = self.list_offsets(topic, partition)
        if offsets.latest < 0:
            return FetchResult(records=(), high_watermark=-1)
        return self.fetch(topic, partition, offsets.latest, max_bytes=max_bytes)

    def metadata(self, topics: Iterable[str] | str | None = None) -> MetadataResult:
        topics_tuple = normalize_topics(()) if topics is None else normalize_topics(topics)
        cached = self._metadata_cache.get(topics_tuple)
        if cached is not None:
            return cached

        if self._transport_name == "http":
            result = self._transport.metadata(topics_tuple)
        else:
            response = decode_metadata_response(
                self._transport.request(API_KEY_METADATA, encode_metadata_request(topics_tuple))
            )
            self._raise_for_error(response.error, "metadata")
            result = MetadataResult(partitions=response.partitions)
        self._metadata_cache.set(topics_tuple, result)
        return result

    def list_offsets(self, topic: str, partition: int) -> ListOffsetsResult:
        if self._transport_name == "http":
            return self._transport.list_offsets(topic, partition)

        response = decode_list_offsets_response(
            self._transport.request(API_KEY_LIST_OFFSETS, encode_list_offsets_request(topic, partition))
        )
        self._raise_for_error(response.error, f"list-offsets {topic}/{partition}")
        return ListOffsetsResult(earliest=response.earliest, latest=response.latest)

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> CommitOffsetResult:
        if self._transport_name == "http":
            return self._transport.commit_offset(group, topic, partition, offset)

        response = decode_commit_offset_response(
            self._transport.request(
                API_KEY_COMMIT_OFFSET,
                encode_commit_offset_request(group, topic, partition, offset),
            )
        )
        self._raise_for_error(response.error, f"commit-offset {group} {topic}/{partition}")
        return CommitOffsetResult(group=group, topic=topic, partition=partition, offset=offset)

    def fetch_committed(self, group: str, topic: str, partition: int) -> FetchCommittedResult:
        if self._transport_name == "http":
            return self._transport.fetch_committed(group, topic, partition)

        response = decode_fetch_committed_response(
            self._transport.request(
                API_KEY_FETCH_COMMITTED,
                encode_fetch_committed_request(group, topic, partition),
            )
        )
        self._raise_for_error(response.error, f"fetch-committed {group} {topic}/{partition}")
        return FetchCommittedResult(offset=response.offset)

    def resolve_consume_offset(
        self,
        group: str,
        topic: str,
        partition: int,
        *,
        start_from: str = "latest",
        start_offset: int = 0,
        use_committed: bool = True,
    ) -> int:
        mode = start_from.strip().lower()
        if mode not in {"latest", "earliest", "offset"}:
            raise ValueError(f"unsupported start_from {start_from!r}")

        if use_committed:
            try:
                return self.fetch_committed(group, topic, partition).offset + 1
            except WaveMQBrokerError:
                pass

        if mode == "offset":
            return int(start_offset)

        offsets = self.list_offsets(topic, partition)
        if mode == "earliest":
            return offsets.earliest

        return 0 if offsets.latest < 0 else offsets.latest + 1

    def consume_poll(
        self,
        group: str,
        topic: str,
        partition: int,
        *,
        next_offset: int | None = None,
        start_from: str = "latest",
        start_offset: int = 0,
        use_committed: bool = True,
        max_messages: int = 0,
        poll_interval: float = 1.0,
        max_bytes: int = 1_048_576,
        commit: bool = True,
        stop_when_caught_up: bool = False,
        on_record: Callable[[Record], None] | None = None,
    ) -> ConsumePollResult:
        if max_messages < 0:
            raise ValueError("max_messages must be >= 0")
        if poll_interval < 0:
            raise ValueError("poll_interval must be >= 0")

        if next_offset is None:
            next_offset = self.resolve_consume_offset(
                group,
                topic,
                partition,
                start_from=start_from,
                start_offset=start_offset,
                use_committed=use_committed,
            )

        started_offset = next_offset
        committed_offset: int | None = None
        high_watermark = -1
        collected: list[Record] = []

        while max_messages <= 0 or len(collected) < max_messages:
            try:
                fetched = self.fetch(topic, partition, next_offset, max_bytes=max_bytes)
            except WaveMQConnectionError:
                time.sleep(poll_interval)
                continue

            high_watermark = fetched.high_watermark
            visible = tuple(record for record in fetched.records if record.offset >= next_offset)

            if not fetched.records:
                if stop_when_caught_up and fetched.high_watermark >= 0 and next_offset > fetched.high_watermark:
                    break
                time.sleep(poll_interval)
                continue

            for record in visible:
                if record.offset < next_offset:
                    continue
                if on_record is not None:
                    on_record(record)
                if commit:
                    self.commit_offset(group, topic, partition, record.offset)
                    committed_offset = record.offset
                next_offset = record.offset + 1
                collected.append(record)
                if max_messages > 0 and len(collected) >= max_messages:
                    break

            if max_messages > 0 and len(collected) >= max_messages:
                break

            if stop_when_caught_up and high_watermark >= 0 and next_offset > high_watermark:
                break

            time.sleep(poll_interval)

        return ConsumePollResult(
            records=tuple(collected),
            start_offset=started_offset,
            next_offset=next_offset,
            high_watermark=high_watermark,
            committed_offset=committed_offset,
        )

    def _raise_for_error(self, error_code: int, action: str) -> None:
        if error_code == 0:
            return
        raise broker_exception_for_error_code(error_code, action, broker=self._transport.broker)


def _parse_broker(broker: str) -> tuple[str, int]:
    broker = broker.strip()
    if broker.startswith("tcp://"):
        broker = broker[6:]
    host, sep, port_text = broker.rpartition(":")
    if not sep or not host or not port_text:
        raise ValueError(f"invalid broker address {broker!r}")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid broker port {port_text!r}") from exc
    if port <= 0 or port > 65535:
        raise ValueError(f"invalid broker port {port}")
    return host, port


def _coerce_records(records: Sequence[Record | bytes | str], key: bytes | str | None = None) -> tuple[Record, ...]:
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    else:
        key_bytes = key

    normalized: list[Record] = []
    for item in records:
        if isinstance(item, Record):
            normalized.append(item)
            continue
        if isinstance(item, str):
            value = item.encode("utf-8")
        else:
            value = item
        normalized.append(Record(key=key_bytes, value=value))

    return tuple(normalized)


def _normalize_http_broker(broker: str) -> str:
    broker = broker.strip()
    if not broker:
        raise ValueError("invalid broker address ''")
    if "://" not in broker:
        broker = "http://" + broker
    return broker.rstrip("/")


def _decode_maybe_base64(value: Any) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value).encode("utf-8")
    if value.startswith("base64:"):
        return base64.b64decode(value[7:])
    return value.encode("utf-8")


def _encode_record_bytes(value: bytes | None) -> str:
    if value is None:
        return ""
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(value).decode("ascii")
    if text.startswith("base64:"):
        return "base64:" + base64.b64encode(value).decode("ascii")
    return text


def _record_from_http(data: dict[str, Any]) -> Record:
    timestamp_text = str(data.get("timestamp", ""))
    if timestamp_text.endswith("Z"):
        timestamp_text = timestamp_text[:-1] + "+00:00"
    timestamp = datetime.fromisoformat(timestamp_text) if timestamp_text else datetime.now(timezone.utc)
    return Record(
        offset=int(data.get("offset", -1)),
        timestamp=timestamp,
        key=_decode_maybe_base64(data.get("key")),
        value=_decode_maybe_base64(data.get("value")),
    )


def _partition_metadata_from_http(topic: str, data: dict[str, Any]) -> PartitionMetadata:
    role_text = str(data.get("role", "leader")).strip().lower()
    role = PartitionRole.LEADER if role_text == "leader" else PartitionRole.FOLLOWER
    replicas = tuple(int(item) for item in data.get("replicas", ()))
    isr = tuple(int(item) for item in data.get("isr", ()))
    return PartitionMetadata(
        topic=topic,
        partition=int(data["id"]),
        broker_id=int(data.get("leader", 0)),
        role=role,
        leader_epoch=int(data.get("leaderEpoch", 0)),
        start_offset=int(data.get("startOffset", 0)),
        high_watermark=int(data.get("highWatermark", -1)),
        leader=int(data.get("leader", 0)),
        replicas=replicas,
        isr=isr,
    )


def _http_error(exc: HTTPError, *, broker: str, action: str) -> Exception:
    raw = exc.read()
    text = raw.decode("utf-8", errors="replace")
    data: dict[str, Any] | None = None
    if text.strip():
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                data = payload
        except json.JSONDecodeError:
            data = None
    message = text.strip() or f"http {exc.code} during {action}"
    error = data.get("error") if data else None
    return broker_exception_for_http(exc.code, message, broker=broker, error=error)
