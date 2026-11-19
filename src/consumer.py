import argparse
import json
import time
from pathlib import Path
from kafka import KafkaConsumer, TopicPartition


def consume(expected, bootstrap, topic, run_id, output, timeout_seconds=120, start_offset=None):
    if expected < 1:
        raise ValueError("expected must be positive")
    
    settings = dict(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=f"gbif-course-{run_id}",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    if start_offset is None:
        client = KafkaConsumer(topic, **settings)
    else:
        if start_offset < 0:
            raise ValueError("start_offset must be non-negative")
        client = KafkaConsumer(**settings)
        partition = TopicPartition(topic, 0)
        client.assign([partition])
        client.seek(partition, start_offset)

    consumer = client
    deadline = time.monotonic() + timeout_seconds
    received = []
    seen = set()
    skipped = 0
    
    try:
        while len(received) < expected and time.monotonic() < deadline:
            for message in consumer:
                envelope = message.value
                if not isinstance(envelope, dict) or envelope.get("run_id") != run_id:
                    skipped += 1
                    continue
                record = envelope.get("record")
                if not isinstance(record, dict) or "key" not in record:
                    raise ValueError("Malformed matching Kafka message")
                # Multiple messages for the run must not be silently discarded.
                identity = (message.topic, message.partition, message.offset)
                if identity in seen:
                    continue
                seen.add(identity)
                received.append(record)
                if len(received) >= expected:
                    break
        if len(received) != expected:
            raise TimeoutError(f"Consumed {len(received)}/{expected} matching messages within {timeout_seconds}s; "
                               f"skipped {skipped} other-run messages")
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            for record in received:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        consumer.close()
    print(f"Kafka consumed: {len(received)} (skipped {skipped} earlier messages)", flush=True)
    return received


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--expected", type=int, required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="gbif_observations")
    p.add_argument("--output", default="data/consumed.jsonl")
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--start-offset", type=int, default=None)
    a = p.parse_args()
    consume(a.expected, a.bootstrap, a.topic, a.run_id, a.output, a.timeout, a.start_offset)
