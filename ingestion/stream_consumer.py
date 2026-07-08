"""
Streaming consumer.

Reads trade events from Kafka and writes them to the data lake (MinIO / S3)
in date-partitioned micro-batches as newline-delimited JSON. This is the
"land raw data" step — no transformation happens here on purpose.

Layout written to the lake:
    raw/trades/dt=YYYY-MM-DD/batch-<epoch_ms>.jsonl

Run:
    python ingestion/stream_consumer.py
"""
import io
import json
import os
import signal
import time
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "10"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "minioadmin")
BUCKET = os.getenv("LAKE_BUCKET", "datalake")

_running = True


def _stop(*_):
    global _running
    _running = False


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3) -> None:
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[consumer] created bucket {BUCKET}")


def flush(s3, buffer: list) -> None:
    if not buffer:
        return
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/trades/dt={dt}/batch-{int(time.time() * 1000)}.jsonl"
    body = "\n".join(json.dumps(rec) for rec in buffer).encode("utf-8")
    s3.upload_fileobj(io.BytesIO(body), BUCKET, key)
    print(f"[consumer] wrote {len(buffer)} records -> s3://{BUCKET}/{key}")


def make_consumer() -> KafkaConsumer:
    for attempt in range(30):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="trades-lake-writer",
                consumer_timeout_ms=1000,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[consumer] Kafka not ready ({exc}); retry {attempt + 1}/30")
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka after 30 attempts")


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    s3 = s3_client()
    ensure_bucket(s3)
    consumer = make_consumer()
    print(f"[consumer] listening topic={TOPIC} batch_size={BATCH_SIZE}")

    buffer: list = []
    last_flush = time.time()
    while _running:
        for message in consumer:
            buffer.append(message.value)
            if len(buffer) >= BATCH_SIZE:
                flush(s3, buffer)
                buffer, last_flush = [], time.time()
            if not _running:
                break
        # Time-based flush so low-traffic periods still land data.
        if time.time() - last_flush >= FLUSH_SECONDS and buffer:
            flush(s3, buffer)
            buffer, last_flush = [], time.time()

    flush(s3, buffer)  # final drain
    print("[consumer] stopped")


if __name__ == "__main__":
    main()
