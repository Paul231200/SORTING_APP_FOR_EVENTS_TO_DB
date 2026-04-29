<div align="center">

# Sauron Redis · Event Processor

**RabbitMQ → Redis → ClickHouse pipeline for IoT / ML operation tracking**

[![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/)
[![Redis](https://img.shields.io/badge/Redis-cache-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-analytics-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com/)

</div>

---

## Overview

This service consumes **machine-learning / IoT events** from a **RabbitMQ** exchange, keeps **in-flight operations** in **Redis**, and persists completed spans to **ClickHouse**. A background worker closes **stale** or **after-hours** operations so analytics stay consistent even when devices miss “end” events.

Typical flow:

```
RabbitMQ (ml_events)
        │
        ▼
   event_processor ──► Redis (active UUIDs / state)
        │
        ▼
   ClickHouse (spk_iot.ml_operations — adjust table/schema to your deployment)
```

---

## Features

- **Topic consumer** (`ml_events` / `ml_event`) with idempotent handling for duplicate starts.
- **Redis** as the source of truth for operations that have started but not finished.
- **ClickHouse** inserts for both live updates and closure of long-running jobs.
- **Working-hours logic** (default: `Asia/Yekaterinburg`, 08:00–19:00) to flag or close operations running outside the business window.
- **Configurable** thresholds via environment variables (`MAX_OPERATION_TIME`, `STALE_CHECK_INTERVAL`, etc.).

---

## Configuration

Secrets and endpoints are **not** committed to the repo. Copy the example env file and fill in your infrastructure:

```bash
cp .env.example .env
```

| Variable | Description |
| :--- | :--- |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` | Redis connection. |
| `AMQP_URL` | Full AMQP URI for RabbitMQ (e.g. `amqp://user:pass@host:5672/vhost`). |
| `CLICKHOUSE_HOST` | ClickHouse server hostname. Port `8123` is used for HTTP API. |
| `DB_USER`, `DB_PASS`, `DB_DATABASE` | ClickHouse credentials and database name. |
| `MAX_OPERATION_TIME` | Hours after which an open operation is force-closed (default: `24`). |
| `STALE_CHECK_INTERVAL` | Seconds between stale-operation sweeps (default: `300`). |
| `MAX_REASONABLE_DURATION` | Minutes; longer runs log a warning (default: `240`). |
| `AFTER_HOURS_OPERATION_CHECK` | Minutes after end of workday before closing stragglers (default: `30`). |

The ClickHouse table expected by the insert path is `spk_iot.ml_operations` with columns compatible with the `INSERT` in `main.py` (adjust if your schema differs).

---

## Run locally

**Python (with Redis & RabbitMQ & ClickHouse already available):**

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)   # or set variables manually
python main.py
```

**Docker Compose (includes Redis; you still need reachable RabbitMQ & ClickHouse):**

```bash
cp .env.example .env
docker compose --env-file .env up --build
```

---

## GitLab CI

`.gitlab-ci.yml` builds and pushes a Docker image using **CI/CD variables** (registry credentials, `REDIS_*`, `AMQP_URL`, `CLICKHOUSE_*`, etc.). Configure those in your GitLab project; do not commit real secrets.

---

## Project layout

| Path | Role |
| :--- | :--- |
| `main.py` | Consumer, Redis state, ClickHouse writes, stale checker thread. |
| `dockerfile` | Application image (runtime env for secrets). |
| `docker-compose.yml` | Local-style stack with Redis + app. |
| `requirements.txt` | Python dependencies. |
| `wait-for-it.sh` | Optional helper to wait for dependencies at startup. |

---

## Author

<p align="center">
  <strong>Pavel I. Barakhnin</strong><br/>
  <em>AI &amp; Computer Vision Developer</em>
</p>

---

## License

Add a `LICENSE` file if you want to specify terms (e.g. MIT, Apache-2.0). Until then, all rights reserved unless you state otherwise.
