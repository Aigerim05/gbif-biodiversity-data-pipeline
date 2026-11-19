"""Run the GBIF -> Kafka -> Pandera -> SQLite -> EDA pipeline."""

import argparse
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from src.analysis import analyze
from src.api_client import ENDPOINT, collect
from src.cleaner import clean
from src.loader import load_twice
from src.producer import get_topic_end_offset, publish
from src.reporter import create_report
from src.validator import run_invalid_tests, validate


def run_kafka(records, bootstrap, topic, run_id):
    
    # Save the topic's end offset before publishing. This avoids rescanning
    # earlier runs and is safe even if the Consumer starts polling later.
    start_offset = get_topic_end_offset(bootstrap, topic)
    output = Path("data/consumed.jsonl")
    command = [
        sys.executable, "-m", "src.consumer",
        "--expected", str(len(records)),
        "--run-id", run_id,
        "--bootstrap", bootstrap,
        "--topic", topic,
        "--output", str(output),
        "--timeout", "180",
        "--start-offset", str(start_offset),
    ]
    process = subprocess.Popen(command)
    try:
        # The saved pre-publish offset prevents missing messages while the
        # Consumer starts. This short pause is only for clearer terminal logs.
        time.sleep(1)
        published = publish(records, bootstrap, topic, run_id)
        exit_code = process.wait(timeout=200)
        if exit_code != 0:
            raise RuntimeError(f"Kafka Consumer exited with status {exit_code}")
    except BaseException:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise

    consumed = [
        json.loads(line)
        for line in output.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if len(consumed) != len(records) or published != len(records):
        raise RuntimeError("Producer/Consumer record counts do not match")
    return consumed, published, start_offset


def main():
    parser = argparse.ArgumentParser(description="GBIF SIS2 (real Kafka)")
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--reuse", action="store_true", help="Replay saved V2 GBIF data")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="gbif_observations")
    parser.add_argument(
        "--database", default="data/output_v2.db",
        help="Use a separate SQLite file for a changed GBIF snapshot",
    )
    args = parser.parse_args()
    if args.count < 200:
        parser.error("Use --count >= 200")

    records = collect(args.count, reuse=args.reuse)
    manifest = json.loads(
        Path("data/collection_manifest.json").read_text(encoding="utf-8")
    )
    run_id = uuid.uuid4().hex
    consumed, published, start_offset = run_kafka(
        records, args.bootstrap, args.topic, run_id
    )

    valid, invalid, failures = validate(consumed)
    invalid_tests = run_invalid_tests(valid) if not valid.empty else {
        "deliberate_test_count": 0, "detected_invalid": 0
    }
    cleaned, cleaning, grouped, pivot = clean(valid)
    cleaned.to_csv("data/cleaned_data.csv", index=False)
    grouped.to_csv("data/grouped_data.csv", index=False)
    pivot.to_csv("data/pivot_data.csv")

    batch, database = load_twice(cleaned, database=args.database)
    analysis = analyze(batch, missing_before=cleaning["missing_before"])
    summary = {
        "run_id": run_id,
        "api_endpoint": ENDPOINT,
        "collection": manifest,
        "reused_cached_source": args.reuse,
        "kafka_start_offset": start_offset,
        "api_collected": len(records),
        "kafka_published": published,
        "kafka_consumed": len(consumed),
        "valid": len(valid),
        "invalid": len(invalid),
        "validation_failures": len(failures),
        "cleaning": cleaning,
        "invalid_tests": invalid_tests,
        "database": database,
        "analysis": analysis,
    }
    Path("data/metrics.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    create_report(summary)

    print("\nEND-TO-END VERIFICATION")
    for key in ("api_collected", "kafka_published", "kafka_consumed", "valid", "invalid"):
        print(f"{key}: {summary[key]}")
    print(f"after_cleaning: {cleaning['after_cleaning']}")
    print(f"sqlite_current_batch: {database['current_batch_stored']}")
    print(f"sqlite_unchanged_on_second_load: {database['unchanged_on_second_load']}")
    print("Report: report/project_report.pdf")


if __name__ == "__main__":
    main()
