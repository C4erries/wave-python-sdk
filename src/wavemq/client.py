from __future__ import annotations

import socket
import threading
import time
from typing import Iterable, Sequence

from .errors import WaveMQConnectionError, WaveMQProtocolError, broker_exception_for_error_code
from .models import (
    CommitOffsetResult,
    CreateTopicResult,
    FetchCommittedResult,
    FetchResult,
    ListOffsetsResult,
    MetadataResult,
    PartitionMetadata,
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
        if transport != "tcp":
            raise ValueError(f"unsupported transport {transport!r}")

        self._broker = broker
        self._timeout = float(timeout)
        self._auto_route = bool(auto_route)
        self._metadata_cache = MetadataCache(metadata_ttl)
        self._transport_name = transport
        self._transport = _TcpTransport(broker, timeout=self._timeout)

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
        started = time.perf_counter()
        payload = encode_ping_request()
        response = decode_ping_response(self._transport.request(API_KEY_PING, payload))
        self._raise_for_error(response.error, "ping")
        return PingResult(rtt_ms=(time.perf_counter() - started) * 1000.0)

    def create_topic(self, topic: str, partitions: int = 1, replication_factor: int = 1) -> CreateTopicResult:
        response = decode_create_topic_response(
            self._transport.request(
                API_KEY_CREATE_TOPIC,
                encode_create_topic_request(topic, partitions, replication_factor),
            )
        )
        self._raise_for_error(response.error, f"create-topic {topic}")
        return CreateTopicResult(topic=topic, partitions=partitions, replication_factor=replication_factor)

    def produce(
        self,
        topic: str,
        partition: int,
        records: Sequence[Record | bytes | str],
        key: bytes | str | None = None,
    ) -> ProduceResult:
        encoded_records = _coerce_records(records, key=key)
        response = decode_produce_response(
            self._transport.request(API_KEY_PRODUCE, encode_produce_request(topic, partition, encoded_records))
        )
        self._raise_for_error(response.error, f"produce {topic}/{partition}")
        return ProduceResult(base_offset=response.base_offset)

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_bytes: int = 1_048_576,
    ) -> FetchResult:
        response = decode_fetch_response(
            self._transport.request(API_KEY_FETCH, encode_fetch_request(topic, partition, offset, max_bytes))
        )
        self._raise_for_error(response.error, f"fetch {topic}/{partition}")
        return FetchResult(records=response.records, high_watermark=response.high_watermark)

    def metadata(self, topics: Iterable[str] | str | None = None) -> MetadataResult:
        topics_tuple = normalize_topics(()) if topics is None else normalize_topics(topics)
        cached = self._metadata_cache.get(topics_tuple)
        if cached is not None:
            return cached

        response = decode_metadata_response(
            self._transport.request(API_KEY_METADATA, encode_metadata_request(topics_tuple))
        )
        self._raise_for_error(response.error, "metadata")
        result = MetadataResult(partitions=response.partitions)
        self._metadata_cache.set(topics_tuple, result)
        return result

    def list_offsets(self, topic: str, partition: int) -> ListOffsetsResult:
        response = decode_list_offsets_response(
            self._transport.request(API_KEY_LIST_OFFSETS, encode_list_offsets_request(topic, partition))
        )
        self._raise_for_error(response.error, f"list-offsets {topic}/{partition}")
        return ListOffsetsResult(earliest=response.earliest, latest=response.latest)

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> CommitOffsetResult:
        response = decode_commit_offset_response(
            self._transport.request(
                API_KEY_COMMIT_OFFSET,
                encode_commit_offset_request(group, topic, partition, offset),
            )
        )
        self._raise_for_error(response.error, f"commit-offset {group} {topic}/{partition}")
        return CommitOffsetResult(group=group, topic=topic, partition=partition, offset=offset)

    def fetch_committed(self, group: str, topic: str, partition: int) -> FetchCommittedResult:
        response = decode_fetch_committed_response(
            self._transport.request(
                API_KEY_FETCH_COMMITTED,
                encode_fetch_committed_request(group, topic, partition),
            )
        )
        self._raise_for_error(response.error, f"fetch-committed {group} {topic}/{partition}")
        return FetchCommittedResult(offset=response.offset)

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
