
import argparse
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from src.api_client import select_fields


def ensure_topic(bootstrap, topic):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15000)
    try:
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)],
                                validate_only=False)
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def get_topic_end_offset(bootstrap, topic):
    
    ensure_topic(bootstrap, topic)
    observer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=15000)
    try:
        partition = TopicPartition(topic, 0)
        return observer.end_offsets([partition])[partition]
    finally:
        observer.close()


def publish(records, bootstrap, topic, run_id):
    ensure_topic(bootstrap, topic)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=4,
                             value_serializer=lambda value: json.dumps(value, ensure_ascii=False,
                                                                          default=str).encode("utf-8"))
    acknowledged = 0
    try:
        for item in records:
            message = {"run_id": run_id, "record": select_fields(item)}
            producer.send(topic, value=message).get(timeout=30)
            acknowledged += 1
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    print(f"Kafka published (acknowledged): {acknowledged}", flush=True)
    return acknowledged


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw_data.json")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="gbif_observations")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    from pathlib import Path
    records = json.loads(Path(args.input).read_text(encoding="utf-8"))
    publish(records, args.bootstrap, args.topic, args.run_id)
