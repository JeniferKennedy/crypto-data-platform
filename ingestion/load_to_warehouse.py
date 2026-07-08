"""
Warehouse loader.

Reads raw JSONL files from the data lake (MinIO / S3) and loads them into
the Postgres warehouse `raw` schema. dbt takes over from there.

Loads two sources:
    raw.trades   <- raw/trades/**
    raw.klines   <- raw/klines/**

Idempotent: truncates and reloads each table on every run so it's safe to
re-run from Airflow.

Run:
    python ingestion/load_to_warehouse.py
"""
import json
import os

import boto3
import psycopg2
from botocore.client import Config
from psycopg2.extras import execute_values

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "minioadmin")
BUCKET = os.getenv("LAKE_BUCKET", "datalake")

PG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "warehouse"),
    "user": os.getenv("PG_USER", "dbt"),
    "password": os.getenv("PG_PASSWORD", "dbt"),
}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def read_jsonl(s3, prefix: str) -> list:
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode("utf-8")
            for line in body.splitlines():
                if line.strip():
                    records.append(json.loads(line))
    return records


DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.trades (
    event_id   TEXT,
    symbol     TEXT,
    price      NUMERIC,
    quantity   NUMERIC,
    side       TEXT,
    event_time TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw.klines (
    symbol    TEXT,
    open_time DATE,
    open      NUMERIC,
    high      NUMERIC,
    low       NUMERIC,
    close     NUMERIC,
    volume    NUMERIC
);
"""


def load_trades(cur, records: list) -> None:
    cur.execute("TRUNCATE raw.trades;")
    if not records:
        return
    rows = [
        (r.get("event_id"), r.get("symbol"), r.get("price"),
         r.get("quantity"), r.get("side"), r.get("event_time"))
        for r in records
    ]
    execute_values(
        cur,
        "INSERT INTO raw.trades (event_id, symbol, price, quantity, side, event_time) VALUES %s",
        rows,
    )


def load_klines(cur, records: list) -> None:
    cur.execute("TRUNCATE raw.klines;")
    if not records:
        return
    rows = [
        (r["symbol"], r["open_time"], r["open"], r["high"],
         r["low"], r["close"], r["volume"])
        for r in records
    ]
    execute_values(
        cur,
        "INSERT INTO raw.klines (symbol, open_time, open, high, low, close, volume) VALUES %s",
        rows,
    )


def main() -> None:
    s3 = s3_client()
    trades = read_jsonl(s3, "raw/trades/")
    klines = read_jsonl(s3, "raw/klines/")
    print(f"[load] read {len(trades)} trades, {len(klines)} klines from lake")

    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            load_trades(cur, trades)
            load_klines(cur, klines)
        conn.commit()
        print("[load] committed to warehouse raw schema")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
