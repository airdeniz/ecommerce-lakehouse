# E-Commerce Real-Time Pipeline

A real-time e-commerce data pipeline built with open-source tools on a self-hosted lakehouse. Designed as a portfolio project targeting Turkish e-commerce companies (Trendyol, n11, Hepsiburada).

## Architecture

```
Postgres → Debezium (CDC) → Kafka → PySpark → MinIO (Iceberg) → dbt → Superset
                                                                    ↑
                                                                 Airflow
```

## Stack

| Tool | Role |
|------|------|
| Postgres | Operational database (WAL-enabled) |
| Debezium | CDC — captures row-level changes from Postgres WAL |
| Kafka (KRaft) | Message broker (no Zookeeper) |
| Redpanda Console | Kafka UI |
| PySpark | Stream processing — Kafka → Iceberg bronze tables |
| MinIO | S3-compatible object storage (lakehouse) |
| Apache Iceberg | Open table format |
| dbt Core | Transformation (staging → silver → gold) |
| Spark Thrift Server | SQL interface for dbt and Superset |
| Airflow | Orchestration — runs dbt nightly at 02:00 |
| Superset | Dashboard |

## Data Layers

| Layer | Schema | Description |
|-------|--------|-------------|
| Bronze | `lakehouse` (Iceberg) | Raw CDC events from Kafka |
| Staging | `ecommerce_staging` | Cleaned views |
| Silver | `ecommerce_silver` | Core business tables |
| Gold | `ecommerce_gold` | Aggregated mart tables for reporting |

## Project Phases

- [x] Phase 1 — CDC Pipeline: Postgres + Debezium + Kafka + Order Generator
- [x] Phase 2 — Stream Processing: PySpark → MinIO (Iceberg)
- [x] Phase 3 — Lakehouse: dbt (staging → silver → gold)
- [x] Phase 4 — Orchestration: Airflow DAG (nightly dbt run)
- [x] Phase 5 — Dashboard: Superset (Daily Revenue, Sales by Category)

## Getting Started

### Prerequisites

- Docker + Docker Compose
- 16GB+ RAM recommended

### Run

```bash
git clone https://github.com/airdeniz/ecommerce-realtime-pipeline.git
cd ecommerce-realtime-pipeline
cp .env.example .env
docker compose up -d
```

### Initialize Superset (first run only)

```bash
docker exec ecom-superset superset db upgrade
docker exec ecom-superset superset init
docker exec ecom-superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin
```

Then connect Superset to Spark Thrift Server:
- Settings → Database Connections → + Database → Apache Hive
- SQLAlchemy URI: `hive://spark-thrift:10000`

### Verify

Open Redpanda Console at `http://localhost:8081` — you should see `ecom.public.orders` topic receiving messages.

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Redpanda Console | http://localhost:8081 | — |
| Airflow | http://localhost:8082 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Thrift Server | localhost:10000 | — |
| Debezium REST API | http://localhost:8083 | — |
| Superset | http://localhost:8088 | admin / admin |

> Debezium connector is registered automatically on startup via the `connector-init` service.

## dbt Models

```
models/
├── staging/          → ecommerce_staging (views)
│   ├── stg_orders
│   ├── stg_order_items
│   ├── stg_products
│   └── stg_users
├── core/             → ecommerce_silver (tables)
│   ├── core_orders
│   └── core_order_items
└── mart/             → ecommerce_gold (tables)
    ├── mart_daily_revenue
    └── mart_sales_by_category
```