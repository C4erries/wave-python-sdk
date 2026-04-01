from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Tuple

CONTENT_TYPE_HEADER = "content-type"


class PartitionRole(IntEnum):
    LEADER = 0
    FOLLOWER = 1


@dataclass(frozen=True, slots=True)
class Header:
    key: str
    value: bytes | None = None


@dataclass(frozen=True, slots=True)
class Record:
    offset: int = -1
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    key: bytes | None = None
    value: bytes | None = None
    headers: Tuple[Header, ...] = ()
    content_type: str | None = None
    crc32c: int = 0


@dataclass(frozen=True, slots=True)
class PartitionMetadata:
    topic: str
    partition: int
    broker_id: int
    role: PartitionRole
    leader_epoch: int
    start_offset: int
    high_watermark: int
    leader: int
    replicas: Tuple[int, ...] = ()
    isr: Tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class PingResult:
    rtt_ms: float


@dataclass(frozen=True, slots=True)
class CreateTopicResult:
    topic: str
    partitions: int
    replication_factor: int


@dataclass(frozen=True, slots=True)
class EnsureTopicResult:
    topic: str
    partitions: int
    replication_factor: int
    created: bool


@dataclass(frozen=True, slots=True)
class ProduceResult:
    base_offset: int
    partition: int | None = None


@dataclass(frozen=True, slots=True)
class FetchResult:
    records: Tuple[Record, ...]
    high_watermark: int


@dataclass(frozen=True, slots=True)
class MetadataResult:
    partitions: Tuple[PartitionMetadata, ...]


@dataclass(frozen=True, slots=True)
class ListOffsetsResult:
    earliest: int
    latest: int


@dataclass(frozen=True, slots=True)
class CommitOffsetResult:
    group: str
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class FetchCommittedResult:
    offset: int


@dataclass(frozen=True, slots=True)
class ConsumePollResult:
    records: Tuple[Record, ...]
    start_offset: int
    next_offset: int
    high_watermark: int
    committed_offset: int | None = None


def normalize_content_type(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        raise ValueError("content_type must not be empty")

    return normalized


def content_type_from_headers(headers: Tuple[Header, ...]) -> str | None:
    found: str | None = None

    for header in headers:
        if header.key.lower() != CONTENT_TYPE_HEADER:
            continue
        if header.value is None:
            raise ValueError("content-type header must have a value")
        try:
            current = header.value.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("content-type header must be utf-8") from exc
        if not current:
            raise ValueError("content-type header must not be empty")
        if found is None:
            found = current
            continue
        if found != current:
            raise ValueError("conflicting content-type headers")

    return found


def canonicalize_record_content_type(record: Record) -> Record:
    header_content_type = content_type_from_headers(record.headers)
    record_content_type = normalize_content_type(record.content_type)

    if header_content_type is not None and record_content_type is not None and header_content_type != record_content_type:
        raise ValueError("content_type does not match content-type header")

    resolved = record_content_type or header_content_type
    headers = tuple(
        header for header in record.headers if header.key.lower() != CONTENT_TYPE_HEADER
    )
    if resolved is not None:
        headers = headers + (Header(CONTENT_TYPE_HEADER, resolved.encode("utf-8")),)

    return Record(
        offset=record.offset,
        timestamp=record.timestamp,
        key=record.key,
        value=record.value,
        headers=headers,
        content_type=resolved,
        crc32c=record.crc32c,
    )
