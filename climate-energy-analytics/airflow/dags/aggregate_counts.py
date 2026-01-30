import os
import re
from collections import defaultdict

def parse_producer_log(log_path):
    produced_counts = defaultdict(int)
    if not os.path.exists(log_path):
        print(f"Producer log not found: {log_path}")
        return produced_counts
    with open(log_path) as f:
        for line in f:
            m = re.search(r"Produced (\\d+) records? to topic: (\\w+)", line)
            if m:
                count, topic = int(m.group(1)), m.group(2)
                produced_counts[topic] += count
    return produced_counts

def parse_consumer_log(log_path):
    written_counts = defaultdict(int)
    if not os.path.exists(log_path):
        print(f"Consumer log not found: {log_path}")
        return written_counts
    with open(log_path) as f:
        for line in f:
            m = re.search(r"Wrote (\\d+) records to s3://[\\w-]+/(\\w+)/", line)
            if m:
                count, topic = int(m.group(1)), m.group(2)
                written_counts[topic] += count
    return written_counts

def aggregate_and_display_counts(producer_log, consumer_log):
    produced = parse_producer_log(producer_log)
    written = parse_consumer_log(consumer_log)
    print("=== Data Production and S3 Write Summary ===")
    topics = set(produced) | set(written)
    for topic in sorted(topics):
        print(f"Topic: {topic}")
        print(f"  Produced: {produced.get(topic, 0)} records")
        print(f"  Written to S3: {written.get(topic, 0)} records\n")

if __name__ == "__main__":
    # Example log paths (update as needed)
    aggregate_and_display_counts("/opt/airflow/logs/producer.log", "/opt/airflow/logs/consumer.log")
