from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Dict, Iterable, Tuple

from .models import MetadataResult


def normalize_topics(topics: Iterable[str] | str) -> Tuple[str, ...]:
    if isinstance(topics, str):
        return (topics,)
    return tuple(topics)


@dataclass
class _CacheEntry:
    value: MetadataResult
    expires_at: float


class MetadataCache:
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = max(0.0, float(ttl_seconds))
        self._entries: Dict[Tuple[str, ...], _CacheEntry] = {}

    def get(self, topics: Tuple[str, ...]) -> MetadataResult | None:
        entry = self._entries.get(topics)
        if entry is None:
            return None
        if entry.expires_at < monotonic():
            self._entries.pop(topics, None)
            return None
        return entry.value

    def set(self, topics: Tuple[str, ...], value: MetadataResult) -> None:
        self._entries[topics] = _CacheEntry(
            value=value,
            expires_at=monotonic() + self._ttl_seconds,
        )

    def clear(self) -> None:
        self._entries.clear()

