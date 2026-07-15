# DataHub — Catalog & Lineage Layer (optional)

Adds a metadata catalog and end-to-end lineage graph on top of the pipeline. It
reads metadata from every layer — the source OLTP DB, the Kafka CDC topics, the
Iceberg lakehouse, the dbt models, and the Superset dashboards — and stitches
them into one searchable graph:

```
Postgres.public.orders → Kafka(ecom.public.orders) → lakehouse.bronze.orders
   → stg_orders → silver.core_orders → gold.mart_daily_revenue → Superset chart
```

DataHub is **read-only observability**. It never writes to the pipeline, so it
does not touch the "two concurrent Iceberg writers" invariant. The catalog is
**derived metadata** — like silver/gold are rebuildable from bronze, the whole
catalog is re-ingestable from the sources, so losing DataHub's volumes on a
`down -v` costs nothing but a re-ingest.

## Why it's a separate compose file

DataHub adds ~5 heavy containers (Elasticsearch + MySQL + GMS + frontend +
setup jobs) and ~2–3 GB RAM. Keeping it in its own file means the default
`docker compose up` stays lean; you opt in explicitly. The file is always run
**merged** with the main one so both share a single Compose project and network
— that shared network is what lets DataHub reach `kafka`, `postgres`,
`spark-thrift`, and `superset` by service name.

It reuses the existing 3-broker Kafka (no second Kafka/Zookeeper) and DataHub's
**internal** schema registry (served by GMS), so no Confluent Schema Registry
container is needed. DataHub's own Kafka topics are created with **RF=3** — this
cluster runs `min.insync.replicas=2`, so RF=1 topics would fail every produce
with `NOT_ENOUGH_REPLICAS` (the same trap the Debezium `connect` service
documents for its storage topics).

## Prerequisites

- The main stack is already up (`docker compose up -d`) and healthy.
- At least one `dbt run` + `dbt docs generate` has happened, so
  `dbt/target/{manifest,catalog}.json` exist for the dbt recipe. The Airflow
  `dbt_pipeline` DAG does both (`dbt_run → dbt_test → dbt_docs_generate`); to
  force it now:
  ```bash
  docker exec ecom-airflow-scheduler bash -lc \
    "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt"
  ```
  (If you start DataHub before this, every other recipe still ingests; only the
  dbt recipe is skipped until the artifacts exist.)
- ~4 GB free RAM on top of the main stack. On Docker Desktop bump the memory
  limit if GMS gets OOM-killed.

## Start it

```bash
docker compose -f docker-compose.yml -f docker-compose.datahub.yml up -d
```

First boot pulls the DataHub images and runs the setup jobs; GMS can take a few
minutes to become healthy. Watch it:

```bash
docker logs -f ecom-datahub-gms          # wait for "Started ... GMS"
docker logs -f ecom-datahub-ingestion    # per-recipe ingestion output
```

Then open the UI: **http://localhost:9002** — default login `datahub` / `datahub`.
(GMS also listens on host port **8084**; 8080/8083 are already taken by the main
stack.)

## Refresh the catalog

Ingestion runs once automatically (the `datahub-ingestion` service, like
`connector-init`). Re-run it any time after new dbt runs or schema changes:

```bash
docker compose -f docker-compose.yml -f docker-compose.datahub.yml \
  run --rm datahub-ingestion
```

Or run a single source:

```bash
docker compose -f docker-compose.yml -f docker-compose.datahub.yml \
  run --rm --entrypoint "datahub ingest -c /recipes/dbt.yml" datahub-ingestion
```

## Stop it (without touching the main stack)

```bash
docker compose -f docker-compose.yml -f docker-compose.datahub.yml stop \
  datahub-frontend datahub-gms datahub-elasticsearch datahub-mysql
```

Full teardown of just the DataHub volumes:

```bash
docker compose -f docker-compose.yml -f docker-compose.datahub.yml down
docker volume rm ecommerce-realtime-pipeline_datahub_es_data \
                 ecommerce-realtime-pipeline_datahub_mysql_data
```

## Recipes (`datahub/recipes/`)

| Recipe | Source | What it catalogs |
|--------|--------|------------------|
| `postgres.yml` | source OLTP `postgres:5432` | `public.*` tables/views (CDC origin) |
| `kafka.yml` | `kafka:9092` cluster | `ecom.public.*` CDC topics |
| `lakehouse_hive.yml` | Spark Thrift `spark-thrift:10000` | bronze/silver/gold/meta/ops Iceberg tables |
| `dbt.yml` | `dbt/target/*.json` | model lineage, tests-as-assertions, docs |
| `superset.yml` | `superset:8088` | dashboards + charts → dataset lineage |

**Ingestion order matters:** physical datasets (postgres, kafka, lakehouse) are
ingested before dbt so dbt's model→table upstream edges point at datasets that
already exist. `ingest-all.sh` enforces this and continues past any single
recipe failure.

## Design notes

- **Lakehouse over Spark Thrift, not the native Iceberg source.** The
  `lakehouse_hive.yml` recipe reaches the Iceberg tables over the same
  HiveServer2 path dbt/Superset/MCP already use — a proven access route that
  needs no extra S3/catalog credential wiring. Tables land under the `hive`
  platform.
- **`target_platform: hive` in the dbt recipe.** Matches the lakehouse recipe's
  platform so dbt model lineage stitches onto the physical tables rather than
  producing an orphaned parallel graph.
- **No `datahub-actions` container.** UI-driven ingestion scheduling and the
  Slack/action framework are omitted to keep the footprint down; ingestion is
  driven from the CLI/compose instead.
