"""
Streaming producer.

Publishes crypto "trade" events to a Kafka topic. Two modes:

  SOURCE=generator  (default)  -> synthetic events, always works, no network needed
  SOURCE=binance               -> real live trades from Binance's public websocket

The generator is the default so the pipeline runs anywhere. Switch to binance
once you've confirmed the plumbing works end to end.

Run:
    python ingestion/stream_producer.py
"""
import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades")
SOURCE = os.getenv("SOURCE", "generator")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"]

# Rough starting prices so synthetic data looks believable.
_BASE_PRICE = {"BTCUSDT": 68000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0, "ADAUSDT": 0.45}

_running = True


def _stop(*_):
    global _running
    _running = False


def make_producer() -> KafkaProducer:
    """Retry until Kafka is reachable (it may still be starting up)."""
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                linger_ms=50,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[producer] Kafka not ready ({exc}); retry {attempt + 1}/30")
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka after 30 attempts")


def run_generator(producer: KafkaProducer) -> None:
    prices = dict(_BASE_PRICE)
    count = 0
    while _running:
        symbol = random.choice(SYMBOLS)
        # Random walk the price a little.
        prices[symbol] *= 1 + random.uniform(-0.0008, 0.0008)
        event = {
            "event_id": f"{symbol}-{int(time.time() * 1000)}-{count}",
            "symbol": symbol,
            "price": round(prices[symbol], 4),
            "quantity": round(random.uniform(0.001, 5.0), 6),
            "side": random.choice(["buy", "sell"]),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, key=symbol, value=event)
        count += 1
        if count % 100 == 0:
            print(f"[producer] sent {count} events")
        time.sleep(0.05)  # ~20 events/sec
    producer.flush()
    print(f"[producer] stopped after {count} events")


def run_binance(producer: KafkaProducer) -> None:
    """Real trades via Binance public websocket. Needs: pip install websocket-client."""
    import websocket  # imported lazily so generator mode has no extra deps

    streams = "/".join(f"{s.lower()}@trade" for s in SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    def on_message(_ws, message):
        payload = json.loads(message)["data"]
        event = {
            "event_id": f"{payload['s']}-{payload['t']}",
            "symbol": payload["s"],
            "price": float(payload["p"]),
            "quantity": float(payload["q"]),
            "side": "sell" if payload["m"] else "buy",
            "event_time": datetime.fromtimestamp(
                payload["T"] / 1000, tz=timezone.utc
            ).isoformat(),
        }
        producer.send(TOPIC, key=payload["s"], value=event)

    def on_error(_ws, err):
        print(f"[producer] ws error: {err}")

    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error)
    print(f"[producer] connecting to Binance: {url}")
    ws.run_forever()


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    producer = make_producer()
    print(f"[producer] source={SOURCE} topic={TOPIC} bootstrap={BOOTSTRAP}")
    if SOURCE == "binance":
        run_binance(producer)
    else:
        run_generator(producer)


if __name__ == "__main__":
    main()
