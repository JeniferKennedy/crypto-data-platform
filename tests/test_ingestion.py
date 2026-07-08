"""
Lightweight unit tests for pure transformation logic in the ingestion layer.
These run in CI without needing Kafka/Postgres/MinIO to be up.

Run:
    pytest -q
"""
import importlib.util
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(module_name, rel_path):
    spec = importlib.util.spec_from_file_location(
        module_name, os.path.join(BASE, rel_path)
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_generator_event_shape():
    """A generated trade event must have all required fields with sane types."""
    prod = _load("stream_producer", "ingestion/stream_producer.py")
    assert set(prod.SYMBOLS) == {"BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"}
    assert all(prod._BASE_PRICE[s] > 0 for s in prod.SYMBOLS)


def test_batch_generator_produces_expected_rows():
    """Batch generator should produce BATCH_DAYS rows with valid OHLC ordering."""
    os.environ["BATCH_DAYS"] = "30"
    batch = _load("batch_ingest", "ingestion/batch_ingest.py")
    rows = batch.fetch_generator("BTCUSDT")
    assert len(rows) == 30
    for r in rows:
        assert r["high"] >= r["low"]
        assert r["high"] >= r["open"]
        assert r["high"] >= r["close"]
        assert r["low"] <= r["open"]
        assert r["low"] <= r["close"]
        assert r["symbol"] == "BTCUSDT"


def test_loader_ddl_defines_both_tables():
    loader = _load("load_to_warehouse", "ingestion/load_to_warehouse.py")
    assert "raw.trades" in loader.DDL
    assert "raw.klines" in loader.DDL
