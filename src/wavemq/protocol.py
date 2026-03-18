from __future__ import annotations

import calendar
import io
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import BinaryIO, Sequence

from .errors import WaveMQProtocolError
from .models import Header, MetadataResult, PartitionMetadata, PartitionRole, Record

API_KEY_CREATE_TOPIC = 0
API_KEY_PRODUCE = 1
API_KEY_FETCH = 2
API_KEY_LIST_OFFSETS = 3
API_KEY_COMMIT_OFFSET = 4
API_KEY_FETCH_COMMITTED = 5
API_KEY_METADATA = 6
API_KEY_PING = 7

CURRENT_VERSION = 0
FRAME_HEADER_SIZE = 14
MAX_FRAME_LENGTH = 16 << 20
MAX_ITEM_COUNT = 100_000


@dataclass(frozen=True, slots=True)
class PingResponseWire:
    error: int


@dataclass(frozen=True, slots=True)
class CreateTopicResponseWire:
    error: int


@dataclass(frozen=True, slots=True)
class ProduceResponseWire:
    base_offset: int
    error: int


@dataclass(frozen=True, slots=True)
class FetchResponseWire:
    records: tuple[Record, ...]
    high_watermark: int
    error: int


@dataclass(frozen=True, slots=True)
class MetadataResponseWire:
    partitions: tuple[PartitionMetadata, ...]
    error: int


@dataclass(frozen=True, slots=True)
class ListOffsetsResponseWire:
    earliest: int
    latest: int
    error: int


@dataclass(frozen=True, slots=True)
class CommitOffsetResponseWire:
    error: int


@dataclass(frozen=True, slots=True)
class FetchCommittedResponseWire:
    offset: int
    error: int


def encode_request_frame(api_key: int, correlation_id: int, payload: bytes) -> bytes:
    return encode_frame(api_key, correlation_id, payload)


def encode_response_frame(api_key: int, correlation_id: int, payload: bytes) -> bytes:
    return encode_frame(api_key, correlation_id, payload)


def encode_frame(api_key: int, correlation_id: int, payload: bytes) -> bytes:
    payload = payload or b""
    header = struct.pack(
        ">Ihhih",
        0,
        int(api_key),
        CURRENT_VERSION,
        int(correlation_id),
        0,
    )
    frame = bytearray(header)
    frame.extend(payload)
    length = len(frame) - 4
    if length > MAX_FRAME_LENGTH:
        raise WaveMQProtocolError(f"frame length {length} exceeds max {MAX_FRAME_LENGTH}")
    struct.pack_into(">I", frame, 0, length)
    return bytes(frame)


def decode_request_frame(stream: BinaryIO) -> tuple[int, int, bytes]:
    return decode_frame(stream)


def decode_response_frame(stream: BinaryIO) -> tuple[int, int, bytes]:
    return decode_frame(stream)


def decode_frame(stream: BinaryIO) -> tuple[int, int, bytes]:
    header = _read_exact(stream, FRAME_HEADER_SIZE)
    length, api_key, version, correlation_id, _flags = struct.unpack(">Ihhih", header)
    if length < FRAME_HEADER_SIZE - 4:
        raise WaveMQProtocolError(f"invalid frame length {length}")
    if length > MAX_FRAME_LENGTH:
        raise WaveMQProtocolError(f"frame length {length} exceeds max {MAX_FRAME_LENGTH}")
    if version != CURRENT_VERSION:
        raise WaveMQProtocolError(f"unsupported version {version}")
    payload_len = length - (FRAME_HEADER_SIZE - 4)
    payload = _read_exact(stream, payload_len) if payload_len else b""
    return api_key, correlation_id, payload


def encode_ping_request() -> bytes:
    return b""


def encode_ping_response(error: int) -> bytes:
    return struct.pack(">h", int(error))


def decode_ping_response(payload: bytes) -> PingResponseWire:
    (error,) = _read_struct(">h", payload)
    return PingResponseWire(error=error)


def encode_create_topic_request(topic: str, partitions: int, replication_factor: int) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, topic)
    _write_int32(buf, partitions)
    _write_int32(buf, replication_factor)
    return buf.getvalue()


def decode_create_topic_request(payload: bytes) -> tuple[str, int, int]:
    stream = io.BytesIO(payload)
    topic = _read_string(stream)
    partitions = _read_int32(stream)
    replication_factor = _read_int32(stream)
    _ensure_eof(stream)
    return topic, partitions, replication_factor


def encode_create_topic_response(error: int) -> bytes:
    return struct.pack(">h", int(error))


def decode_create_topic_response(payload: bytes) -> CreateTopicResponseWire:
    (error,) = _read_struct(">h", payload)
    return CreateTopicResponseWire(error=error)


def encode_produce_request(topic: str, partition: int, records: Sequence[Record]) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, topic)
    _write_int32(buf, partition)
    _write_int32(buf, len(records))
    for record in records:
        encoded = encode_record(record)
        _write_int32(buf, len(encoded))
        buf.write(encoded)
    return buf.getvalue()


def decode_produce_request(payload: bytes) -> tuple[str, int, tuple[Record, ...]]:
    stream = io.BytesIO(payload)
    topic = _read_string(stream)
    partition = _read_int32(stream)
    record_count = _read_int32(stream)
    _validate_count(record_count, "record")
    records = []
    for _ in range(record_count):
        record_len = _read_int32(stream)
        if record_len < 0:
            raise WaveMQProtocolError("invalid record length")
        records.append(decode_record(io.BytesIO(_read_exact(stream, record_len))))
    _ensure_eof(stream)
    return topic, partition, tuple(records)


def encode_produce_response(base_offset: int, error: int) -> bytes:
    return struct.pack(">qh", int(base_offset), int(error))


def decode_produce_response(payload: bytes) -> ProduceResponseWire:
    base_offset, error = _read_struct(">qh", payload)
    return ProduceResponseWire(base_offset=base_offset, error=error)


def encode_fetch_request(topic: str, partition: int, offset: int, max_bytes: int) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, topic)
    _write_int32(buf, partition)
    _write_int64(buf, offset)
    _write_int32(buf, max_bytes)
    return buf.getvalue()


def decode_fetch_request(payload: bytes) -> tuple[str, int, int, int]:
    stream = io.BytesIO(payload)
    topic = _read_string(stream)
    partition = _read_int32(stream)
    offset = _read_int64(stream)
    max_bytes = _read_int32(stream)
    _ensure_eof(stream)
    return topic, partition, offset, max_bytes


def encode_fetch_response(records: Sequence[Record], high_watermark: int, error: int) -> bytes:
    buf = io.BytesIO()
    _write_int16(buf, error)
    _write_int32(buf, len(records))
    for record in records:
        encoded = encode_record(record)
        _write_int32(buf, len(encoded))
        buf.write(encoded)
    _write_int64(buf, high_watermark)
    return buf.getvalue()


def decode_fetch_response(payload: bytes) -> FetchResponseWire:
    stream = io.BytesIO(payload)
    error = _read_int16(stream)
    record_count = _read_int32(stream)
    _validate_count(record_count, "record")
    records = []
    for _ in range(record_count):
        record_len = _read_int32(stream)
        if record_len < 0:
            raise WaveMQProtocolError("invalid record length")
        records.append(decode_record(io.BytesIO(_read_exact(stream, record_len))))
    high_watermark = _read_int64(stream)
    _ensure_eof(stream)
    return FetchResponseWire(records=tuple(records), high_watermark=high_watermark, error=error)


def encode_metadata_request(topics: Sequence[str]) -> bytes:
    buf = io.BytesIO()
    _write_int32(buf, len(topics))
    for topic in topics:
        _write_string(buf, topic)
    return buf.getvalue()


def decode_metadata_request(payload: bytes) -> tuple[str, ...]:
    stream = io.BytesIO(payload)
    topic_count = _read_int32(stream)
    _validate_count(topic_count, "topic")
    topics = tuple(_read_string(stream) for _ in range(topic_count))
    _ensure_eof(stream)
    return topics


def encode_metadata_response(partitions: Sequence[PartitionMetadata], error: int) -> bytes:
    buf = io.BytesIO()
    _write_int16(buf, error)
    _write_int32(buf, len(partitions))
    for partition in partitions:
        _write_string(buf, partition.topic)
        _write_int32(buf, partition.partition)
        _write_int32(buf, partition.broker_id)
        _write_int16(buf, int(partition.role))
        _write_int32(buf, partition.leader_epoch)
        _write_int64(buf, partition.start_offset)
        _write_int64(buf, partition.high_watermark)
        _write_int32(buf, partition.leader)
        _write_int32(buf, len(partition.replicas))
        for replica_id in partition.replicas:
            _write_int32(buf, replica_id)
        _write_int32(buf, len(partition.isr))
        for isr_id in partition.isr:
            _write_int32(buf, isr_id)
    return buf.getvalue()


def decode_metadata_response(payload: bytes) -> MetadataResponseWire:
    stream = io.BytesIO(payload)
    error = _read_int16(stream)
    partition_count = _read_int32(stream)
    _validate_count(partition_count, "partition")
    partitions = []
    for _ in range(partition_count):
        topic = _read_string(stream)
        partition = _read_int32(stream)
        broker_id = _read_int32(stream)
        role_value = _read_int16(stream)
        try:
            role = PartitionRole(role_value)
        except ValueError as exc:
            raise WaveMQProtocolError(f"invalid partition role {role_value}") from exc
        leader_epoch = _read_int32(stream)
        start_offset = _read_int64(stream)
        high_watermark = _read_int64(stream)
        leader = _read_int32(stream)
        replicas = tuple(_read_int32(stream) for _ in range(_read_int32(stream)))
        isr = tuple(_read_int32(stream) for _ in range(_read_int32(stream)))
        partitions.append(
            PartitionMetadata(
                topic=topic,
                partition=partition,
                broker_id=broker_id,
                role=role,
                leader_epoch=leader_epoch,
                start_offset=start_offset,
                high_watermark=high_watermark,
                leader=leader,
                replicas=replicas,
                isr=isr,
            )
        )
    _ensure_eof(stream)
    return MetadataResponseWire(partitions=tuple(partitions), error=error)


def encode_commit_offset_request(group: str, topic: str, partition: int, offset: int) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, group)
    _write_string(buf, topic)
    _write_int32(buf, partition)
    _write_int64(buf, offset)
    return buf.getvalue()


def decode_commit_offset_request(payload: bytes) -> tuple[str, str, int, int]:
    stream = io.BytesIO(payload)
    group = _read_string(stream)
    topic = _read_string(stream)
    partition = _read_int32(stream)
    offset = _read_int64(stream)
    _ensure_eof(stream)
    return group, topic, partition, offset


def encode_commit_offset_response(error: int) -> bytes:
    return struct.pack(">h", int(error))


def decode_commit_offset_response(payload: bytes) -> CommitOffsetResponseWire:
    (error,) = _read_struct(">h", payload)
    return CommitOffsetResponseWire(error=error)


def encode_fetch_committed_request(group: str, topic: str, partition: int) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, group)
    _write_string(buf, topic)
    _write_int32(buf, partition)
    return buf.getvalue()


def decode_fetch_committed_request(payload: bytes) -> tuple[str, str, int]:
    stream = io.BytesIO(payload)
    group = _read_string(stream)
    topic = _read_string(stream)
    partition = _read_int32(stream)
    _ensure_eof(stream)
    return group, topic, partition


def encode_fetch_committed_response(offset: int, error: int) -> bytes:
    return struct.pack(">qh", int(offset), int(error))


def decode_fetch_committed_response(payload: bytes) -> FetchCommittedResponseWire:
    offset, error = _read_struct(">qh", payload)
    return FetchCommittedResponseWire(offset=offset, error=error)


def encode_list_offsets_request(topic: str, partition: int) -> bytes:
    buf = io.BytesIO()
    _write_string(buf, topic)
    _write_int32(buf, partition)
    return buf.getvalue()


def decode_list_offsets_request(payload: bytes) -> tuple[str, int]:
    stream = io.BytesIO(payload)
    topic = _read_string(stream)
    partition = _read_int32(stream)
    _ensure_eof(stream)
    return topic, partition


def encode_list_offsets_response(earliest: int, latest: int, error: int) -> bytes:
    return struct.pack(">hqq", int(error), int(earliest), int(latest))


def decode_list_offsets_response(payload: bytes) -> ListOffsetsResponseWire:
    error, earliest, latest = _read_struct(">hqq", payload)
    return ListOffsetsResponseWire(earliest=earliest, latest=latest, error=error)


def encode_record(record: Record) -> bytes:
    buf = io.BytesIO()
    timestamp_ns = _datetime_to_unix_nanos(record.timestamp)
    _write_int64(buf, int(record.offset))
    _write_int64(buf, timestamp_ns)
    _write_bytes(buf, record.key)
    _write_bytes(buf, record.value)
    _write_int32(buf, len(record.headers))
    for header in record.headers:
        _write_string(buf, header.key)
        _write_bytes(buf, header.value)
    return buf.getvalue()


def decode_record(stream: BinaryIO) -> Record:
    offset = _read_int64(stream)
    timestamp_ns = _read_int64(stream)
    key = _read_bytes(stream)
    value = _read_bytes(stream)
    headers_count = _read_int32(stream)
    _validate_count(headers_count, "header")
    headers = []
    for _ in range(headers_count):
        headers.append(Header(key=_read_string(stream), value=_read_bytes(stream)))
    timestamp = _unix_nanos_to_datetime(timestamp_ns)
    return Record(offset=offset, timestamp=timestamp, key=key, value=value, headers=tuple(headers))


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    if size < 0:
        raise WaveMQProtocolError("negative size")
    data = stream.read(size)
    if data is None or len(data) != size:
        raise WaveMQProtocolError("unexpected end of stream")
    return data


def _read_struct(fmt: str, payload: bytes) -> tuple[int, ...]:
    size = struct.calcsize(fmt)
    if len(payload) != size:
        raise WaveMQProtocolError(f"expected {size} bytes, got {len(payload)}")
    return struct.unpack(fmt, payload)


def _ensure_eof(stream: BinaryIO) -> None:
    if stream.read(1):
        raise WaveMQProtocolError("unexpected trailing bytes")


def _validate_count(value: int, kind: str) -> None:
    if value < 0:
        raise WaveMQProtocolError(f"negative {kind} count")
    if value > MAX_ITEM_COUNT:
        raise WaveMQProtocolError(f"{kind} count {value} exceeds max {MAX_ITEM_COUNT}")


def _write_string(buf: io.BytesIO, value: str) -> None:
    data = value.encode("utf-8")
    if len(data) > 0x7FFF:
        raise WaveMQProtocolError("string length exceeds int16")
    buf.write(struct.pack(">h", len(data)))
    buf.write(data)


def _read_string(stream: BinaryIO) -> str:
    length = _read_int16(stream)
    if length < 0:
        raise WaveMQProtocolError(f"invalid string length {length}")
    if length == 0:
        return ""
    return _read_exact(stream, length).decode("utf-8")


def _write_bytes(buf: io.BytesIO, value: bytes | None) -> None:
    if value is None:
        buf.write(struct.pack(">i", -1))
        return
    if len(value) > 0x7FFFFFFF:
        raise WaveMQProtocolError("bytes length exceeds int32")
    buf.write(struct.pack(">i", len(value)))
    buf.write(value)


def _read_bytes(stream: BinaryIO) -> bytes | None:
    length = _read_int32(stream)
    if length < 0:
        return None
    if length == 0:
        return b""
    return _read_exact(stream, length)


def _write_int32(buf: io.BytesIO, value: int) -> None:
    buf.write(struct.pack(">i", int(value)))


def _write_int64(buf: io.BytesIO, value: int) -> None:
    buf.write(struct.pack(">q", int(value)))


def _write_int16(buf: io.BytesIO, value: int) -> None:
    buf.write(struct.pack(">h", int(value)))


def _read_int16(stream: BinaryIO) -> int:
    return struct.unpack(">h", _read_exact(stream, 2))[0]


def _read_int32(stream: BinaryIO) -> int:
    return struct.unpack(">i", _read_exact(stream, 4))[0]


def _read_int64(stream: BinaryIO) -> int:
    return struct.unpack(">q", _read_exact(stream, 8))[0]


def _datetime_to_unix_nanos(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    seconds = calendar.timegm(value.utctimetuple())
    return seconds * 1_000_000_000 + value.microsecond * 1_000


def _unix_nanos_to_datetime(value: int) -> datetime:
    seconds, nanos = divmod(int(value), 1_000_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.replace(microsecond=nanos // 1_000)
