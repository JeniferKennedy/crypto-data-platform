# Crypto Data Platform — End-to-End Data Engineering Project

A batch **and** streaming data platform that ingests crypto market data, lands it in a
data lake, loads it into a warehouse, models it into a star schema with dbt, tests data
quality, orchestrates the whole thing with Airflow, and serves it through a dashboard —
all runnable locally with Docker, then optionally moved to the cloud.

This README is a **step-by-step guide you can follow to replicate the project from scratch.**
Every step has the exact commands to run and how to verify it worked before moving on.

---

## Architecture

```
                        ┌─────────────────────────────────────────────┐
   Sources              │   Orchestration: Airflow                     │
   ┌──────────┐         │   Infra as code: Terraform                   │
   │ Binance /│         │   CI/CD: GitHub Actions   ·  Everything = code│
   │ generator│         └─────────────────────────────────────────────┘
   └────┬─────┘
        │
   ┌────▼─────┐   batch    ┌──────────┐   load   ┌────────────┐   dbt    ┌──────────┐
   │ Batch    ├───────────►│          │─────────►│ Warehouse  │─────────►│ Star     │
   │ ingest   │            │ Data lake│          │ (Postgres) │  models  │ schema   │
   └──────────┘            │ (MinIO/  │          └────────────┘  + tests └────┬─────┘
   ┌──────────┐  stream    │  S3)     │                                        │
   │ Kafka    ├───────────►│          │                                        ▼
   │ producer │            └──────────┘                                  ┌──────────┐
   │ +consumer│                                                          │ Metabase │
   └──────────┘                                                          │ dashboard│
                                                                         └──────────┘
```

**Tech stack:** Python · Kafka · MinIO (S3) · Postgres · dbt · Airflow · Metabase · Terraform · Docker · GitHub Actions

---

## Prerequisites

Install these first:

- **Docker Desktop** (includes Docker Compose) — the only hard requirement for the local build
- **Python 3.11+** — to run the ingestion scripts on your host
- **Git** — for version control and pushing to GitHub
- ~8 GB free RAM (Airflow + Kafka + Postgres + Metabase together are a bit hungry)

Verify:
```bash
docker --version
docker compose version
python --version
git --version
```

---

## Repo layout

```
crypto-data-platform/
├── docker-compose.yml          # the entire local stack
├── requirements.txt
├── .env.example
├── ingestion/
│   ├── stream_producer.py      # events  -> Kafka
│   ├── stream_consumer.py      # Kafka   -> data lake
│   ├── batch_ingest.py         # history -> data lake
│   └── load_to_warehouse.py    # lake    -> Postgres raw schema
├── dbt/crypto/                 # transformation + modeling + tests
│   ├── models/staging/         # cleaned views
│   └── models/marts/           # star schema (facts + dimension)
├── airflow/dags/
│   └── crypto_pipeline.py      # orchestrates the batch path
├── terraform/main.tf           # cloud migration reference
├── tests/test_ingestion.py     # unit tests (run in CI)
└── .github/workflows/ci.yml    # CI pipeline
```

---

## Phase-by-phase build

Work through these in order. Each phase ends with a **checkpoint** — don't move on until it passes.

### Phase 0 — Get the code and set up

```bash
# From the unzipped folder:
cd crypto-data-platform

# Python environment for host-side scripts
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Env file
cp .env.example .env

# Version control
git init
git add .
git commit -m "Initial commit: crypto data platform scaffold"
```

**Checkpoint:** `pip list` shows `boto3`, `dbt-postgres`, `kafka-python-ng`.

---

### Phase 1 — Bring up the local stack

```bash
docker compose up -d
docker compose ps        # all services should be "running"/"healthy"
```

This starts MinIO (lake), Postgres (warehouse), Kafka + Zookeeper (streaming),
Airflow (orchestration), and Metabase (BI). First start pulls images and can take
a few minutes.

**Checkpoint — open these in your browser:**
- MinIO console: http://localhost:9001  (login `minioadmin` / `minioadmin`)
- Airflow: http://localhost:8080  (login `admin` / `admin`)
- Metabase: http://localhost:3000  (first-run setup wizard)

If a service is unhealthy, check logs: `docker compose logs kafka` (or the service name).

---

### Phase 2 — Batch ingestion (history → lake)

Pull ~180 days of daily candles per symbol and land them raw in the lake.

```bash
python ingestion/batch_ingest.py
```

**Checkpoint:** In the MinIO console (http://localhost:9001) open the `datalake`
bucket. You should see `raw/klines/symbol=BTCUSDT/klines.jsonl` and one per symbol.

> Uses the synthetic generator by default. To pull **real** data instead:
> `SOURCE=binance python ingestion/batch_ingest.py` (needs internet; Binance may be
> geo-restricted in some regions — the generator is the safe default).

---

### Phase 3 — Streaming ingestion (live events → lake)

Open **two terminals** (both with the venv activated).

Terminal 1 — produce events into Kafka:
```bash
python ingestion/stream_producer.py
# prints "[producer] sent 100 events" ... leave it running
```

Terminal 2 — consume from Kafka and write micro-batches to the lake:
```bash
python ingestion/stream_consumer.py
# prints "[consumer] wrote 500 records -> s3://datalake/raw/trades/dt=.../batch-...jsonl"
```

Let them run for a minute or two, then stop both with `Ctrl+C`.

**Checkpoint:** MinIO now has `raw/trades/dt=YYYY-MM-DD/batch-*.jsonl` files.

> This is your differentiator — most junior portfolios have no streaming path.
> Real live source: run the producer with `SOURCE=binance` to stream actual trades.

---

### Phase 4 — Load into the warehouse

Pull the raw files from the lake into Postgres so dbt can transform them.

```bash
python ingestion/load_to_warehouse.py
# "[load] read N trades, M klines from lake"
# "[load] committed to warehouse raw schema"
```

**Checkpoint:** query the warehouse:
```bash
docker compose exec postgres psql -U dbt -d warehouse -c "SELECT count(*) FROM raw.trades;"
docker compose exec postgres psql -U dbt -d warehouse -c "SELECT count(*) FROM raw.klines;"
```

---

### Phase 5 — Transform + model with dbt (star schema)

```bash
cd dbt/crypto
dbt deps  --profiles-dir .      # install dbt_utils
dbt run   --profiles-dir .      # build staging views + marts tables
dbt test  --profiles-dir .      # run all data quality tests
cd ../..
```

`dbt run` builds: staging views (`stg_trades`, `stg_klines`), a dimension
(`dim_symbol`), and two facts (`fct_daily_price`, `fct_trade_activity`) — a
classic star schema.

**Checkpoint:** all tests pass, and:
```bash
docker compose exec postgres psql -U dbt -d warehouse \
  -c "SELECT symbol, count(*) FROM marts.fct_daily_price GROUP BY symbol;"
```

---

### Phase 6 — See data quality catch a problem (do this once)

To prove the tests actually work, break the data on purpose:

```bash
docker compose exec postgres psql -U dbt -d warehouse \
  -c "UPDATE raw.trades SET side = 'HODL' WHERE ctid IN (SELECT ctid FROM raw.trades LIMIT 1);"
cd dbt/crypto && dbt run --profiles-dir . && dbt test --profiles-dir . ; cd ../..
```

The `accepted_values` test on `side` should **fail** — that's the pipeline doing its
job. Re-run `python ingestion/load_to_warehouse.py` to restore clean data.
(Screenshot this failing test for your portfolio — it shows you understand data quality.)

---

### Phase 7 — Orchestrate with Airflow

The DAG `crypto_batch_pipeline` runs ingest → load → dbt run → dbt test on a schedule.

1. Open http://localhost:8080 (admin / admin)
2. Find `crypto_batch_pipeline`, unpause it (toggle on the left)
3. Click the DAG → **Trigger DAG** (▶) → watch tasks go green in Graph view

**Checkpoint:** a fully green DAG run. Screenshot the Graph view — this is
one of the strongest single images in a DE portfolio.

---

### Phase 8 — Serve through a dashboard

1. Open Metabase: http://localhost:3000, finish the setup wizard
2. Add database → PostgreSQL:
   - Host `postgres`, Port `5432`, Database `warehouse`, User `dbt`, Password `dbt`
3. Build 2–3 charts from the `marts` schema, e.g.:
   - Daily close price over time per symbol (from `fct_daily_price`)
   - Trade count by symbol (from `fct_trade_activity`)
   - Buy vs sell ratio per hour

**Checkpoint:** a saved dashboard with a few charts. Screenshot it.

---

### Phase 9 — CI/CD

`.github/workflows/ci.yml` runs unit tests and validates dbt on every push.

```bash
pytest -q                        # runs the same tests CI runs, locally

# Then push to GitHub:
git add .
git commit -m "Working end-to-end pipeline"
git branch -M main
git remote add origin https://github.com/<you>/crypto-data-platform.git
git push -u origin main
```

**Checkpoint:** the Actions tab on GitHub shows a green CI run.

---

### Phase 10 — (Optional) Make it truly cloud

This is what lets you honestly say "cloud" in interviews.

- **Storage:** `cd terraform`, set a unique bucket name in `main.tf`, then with AWS
  credentials configured: `terraform init && terraform plan && terraform apply`.
  Point the scripts at real S3 by removing `S3_ENDPOINT` (boto3 then uses AWS).
- **Warehouse:** swap Postgres for **Snowflake** (free trial) or **BigQuery** (free
  tier) by changing `dbt/crypto/profiles.yml` to the matching adapter.
- **Orchestration:** run Airflow on **Astronomer** (free tier) or AWS **MWAA**.

Do storage first — it's the cheapest, lowest-risk migration and gives you a real
cloud credential to talk about.

---

## Common issues

- **Kafka connection refused:** the broker takes ~30s to be ready; the scripts retry
  automatically. If it persists, `docker compose restart kafka`.
- **Airflow tasks fail with import errors:** the container installs deps on first boot
  via `_PIP_ADDITIONAL_REQUIREMENTS`; give it a minute after `docker compose up`.
- **`dbt` not found:** activate the venv (`source .venv/bin/activate`) or run dbt
  inside the Airflow container.
- **Ports already in use:** something else is on 5432/8080/9000/3000 — stop it or
  change the port mapping in `docker-compose.yml`.

Tear everything down (and wipe data) with: `docker compose down -v`

---
