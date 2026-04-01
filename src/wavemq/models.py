from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Tuple


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
