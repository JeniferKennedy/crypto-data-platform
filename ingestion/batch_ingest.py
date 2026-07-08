"""
Batch ingestion.

Pulls historical daily OHLCV candles ("klines") for each symbol and lands them
raw in the data lake, partitioned by symbol. This is the batch counterpart to the
streaming path — same lake, different cadence.

Two modes:
  SOURCE=generator (default) -> synthetic daily candles, always works
  SOURCE=binance             -> real klines from Binance public REST API

Layout written to the lake:
    raw/klines/symbol=<SYMBOL>/klines.jsonl

Run:
    python ingestion/batch_ingest.py
"""
import io
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import boto3
import requests
from botocore.client import Config

SOURCE = os.getenv("SOURCE", "generator")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"]
DAYS = int(os.getenv("BATCH_DAYS", "180"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "minioadmin")
BUCKET = os.getenv("LAKE_BUCKET", "datalake")

_BASE_PRICE = {"BTCUSDT": 68000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0, "ADAUSDT": 0.45}


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


def fetch_generator(symbol: str) -> list:
    rows, price = [], _BASE_PRICE[symbol]
    start = datetime.now(timezone.utc) - timedelta(days=DAYS)
    for i in range(DAYS):
        day = start + timedelta(days=i)
        open_p = price
        close_p = open_p * (1 + random.uniform(-0.05, 0.05))
        high_p = max(open_p, close_p) * (1 + random.uniform(0, 0.03))
        low_p = min(open_p, close_p) * (1 - random.uniform(0, 0.03))
        rows.append({
            "symbol": symbol,
            "open_time": day.strftime("%Y-%m-%d"),
            "open": round(open_p, 4),
            "high": round(high_p, 4),
            "low": round(low_p, 4),
            "close": round(close_p, 4),
            "volume": round(random.uniform(1000, 50000), 2),
        })
        price = close_p
    return rows


def fetch_binance(symbol: str) -> list:
    url = "https://api.binance.com/api/v3/klines"
    resp = requests.get(
        url, params={"symbol": symbol, "interval": "1d", "limit": DAYS}, timeout=20
    )
    resp.raise_for_status()
    rows = []
    for k in resp.json():
        rows.append({
            "symbol": symbol,
            "open_time": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return rows


def main() -> None:
    s3 = s3_client()
    ensure_bucket(s3)
    fetch = fetch_binance if SOURCE == "binance" else fetch_generator
    print(f"[batch] source={SOURCE} days={DAYS}")
    for symbol in SYMBOLS:
        rows = fetch(symbol)
        key = f"raw/klines/symbol={symbol}/klines.jsonl"
        body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        s3.upload_fileobj(io.BytesIO(body), BUCKET, key)
        print(f"[batch] {symbol}: {len(rows)} rows -> s3://{BUCKET}/{key}")
        if SOURCE == "binance":
            time.sleep(0.5)  # be polite to the API


if __name__ == "__main__":
    main()
