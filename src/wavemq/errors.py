from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class WaveMQError(Exception):
    message: str
    broker: Optional[str] = None
    error_code: Optional[int] = None

    def __str__(self) -> str:
        parts = [self.message]
        if self.error_code is not None:
            parts.append(f"error_code={self.error_code}")
        if self.broker is not None:
            parts.append(f"broker={self.broker}")
        return " ".join(parts)


class WaveMQConnectionError(WaveMQError):
    pass


class WaveMQProtocolError(WaveMQError):
    pass


class WaveMQBrokerError(WaveMQError):
    pass


class TopicNotFoundError(WaveMQBrokerError):
    pass


class PartitionNotFoundError(WaveMQBrokerError):
    pass


class TopicExistsError(WaveMQBrokerError):
    pass


class NotLeaderError(WaveMQBrokerError):
    pass


class InvalidRequestError(WaveMQBrokerError):
    pass


def broker_exception_for_error_code(
    error_code: int,
    message: str,
    broker: Optional[str] = None,
) -> WaveMQError:
    mapping = {
        0: None,
        1: WaveMQBrokerError,
        2: InvalidRequestError,
        3: TopicNotFoundError,
        4: PartitionNotFoundError,
        5: TopicExistsError,
        6: NotLeaderError,
        7: WaveMQBrokerError,
    }
    exc_type = mapping.get(error_code, WaveMQBrokerError)
    if exc_type is None:
        return WaveMQError("ok", broker=broker, error_code=error_code)
    return exc_type(message, broker=broker, error_code=error_code)

