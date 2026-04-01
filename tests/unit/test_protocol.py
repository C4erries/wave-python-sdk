from __future__ import annotations

import io
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from wavemq.models import Header, PartitionMetadata, PartitionRole, Record  # noqa: E402
from wavemq.protocol import (  # noqa: E402
    CURRENT_VERSION,
    API_KEY_PRODUCE,
    decode_create_topic_request,
    decode_frame,
    decode_metadata_request,
    decode_metadata_response,
    decode_produce_request,
    decode_record,
    encode_create_topic_request,
    encode_frame,
    encode_metadata_request,
    encode_metadata_response,
    encode_produce_request,
    encode_record,
    encode_response_frame,
)


class ProtocolTests(unittest.TestCase):
    def test_frame_roundtrip(self) -> None:
        payload = b"hello"
        frame = encode_frame(API_KEY_PRODUCE, 42, payload)
        api_key, correlation_id, decoded = decode_frame(io.BytesIO(frame))
        self.assertEqual(API_KEY_PRODUCE, api_key)
        self.assertEqual(42, correlation_id)
        self.assertEqual(payload, decoded)

    def test_record_roundtrip(self) -> None:
        record = Record(
            offset=17,
            timestamp=datetime(2026, 3, 18, 9, 0, 1, 123456, tzinfo=timezone.utc),
            key=b"k",
            value=b"v",
            headers=(Header("h1", b"hv"), Header("h2", None)),
            content_type="application/x.float64",
        )
        payload = encode_record(record)
        decoded = decode_record(io.BytesIO(payload))
        self.assertEqual(record.offset, decoded.offset)
        self.assertEqual(record.key, decoded.key)
        self.assertEqual(record.value, decoded.value)
        self.assertEqual(
            (Header("h1", b"hv"), Header("h2", None), Header("content-type", b"application/x.float64")),
            decoded.headers,
        )
        self.assertEqual("application/x.float64", decoded.content_type)
        self.assertEqual(record.timestamp, decoded.timestamp)

    def test_record_roundtrip_rejects_conflicting_content_type(self) -> None:
        record = Record(
            key=b"k",
            value=b"v",
            headers=(Header("content-type", b"text/plain"),),
            content_type="application/json",
        )
        with self.assertRaises(ValueError):
            encode_record(record)

    def test_request_encoding_decoding(self) -> None:
        payload = encode_create_topic_request("demo", 3, 2)
        topic, partitions, replication_factor = decode_create_topic_request(payload)
        self.assertEqual(("demo", 3, 2), (topic, partitions, replication_factor))

        payload = encode_produce_request("demo", 1, (Record(value=b"a"), Record(value=b"b")))
        topic, partition, records = decode_produce_request(payload)
        self.assertEqual("demo", topic)
        self.assertEqual(1, partition)
        self.assertEqual(2, len(records))
        self.assertEqual(b"a", records[0].value)

        metadata_payload = encode_metadata_request(("demo", "other"))
        self.assertEqual(("demo", "other"), decode_metadata_request(metadata_payload))

    def test_metadata_response_roundtrip(self) -> None:
        partition = PartitionMetadata(
            topic="demo",
            partition=0,
            broker_id=1,
            role=PartitionRole.LEADER,
            leader_epoch=3,
            start_offset=0,
            high_watermark=2,
            leader=1,
            replicas=(1,),
            isr=(1,),
        )
        payload = encode_metadata_response((partition,), 0)
        decoded = decode_metadata_response(payload)
        self.assertEqual(0, decoded.error)
        self.assertEqual((partition,), decoded.partitions)

    def test_decode_frame_rejects_bad_version(self) -> None:
        frame = bytearray(encode_frame(API_KEY_PRODUCE, 1, b""))
        frame[6:8] = (1).to_bytes(2, "big", signed=True)
        with self.assertRaises(Exception):
            decode_frame(io.BytesIO(bytes(frame)))


if __name__ == "__main__":
    unittest.main()
