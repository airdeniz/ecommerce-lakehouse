#!/bin/bash
# Run every ingestion recipe in order, continuing past any single failure so a
# not-yet-ready source (e.g. dbt/target missing before the first dbt run) does
# not block the rest of the catalog from loading.
#
# Order matters for lineage stitching: the physical datasets (postgres, kafka,
# lakehouse) are ingested BEFORE dbt so dbt's model->table upstream edges point
# at datasets that already exist.
set -u

RECIPE_DIR=/recipes
RECIPES="postgres kafka lakehouse_hive superset dbt"

failed=""
for name in $RECIPES; do
  recipe="$RECIPE_DIR/$name.yml"
  if [ ! -f "$recipe" ]; then
    echo ">>> skip $name (no recipe file)"
    continue
  fi
  echo ">>> ingesting: $name"
  if datahub ingest -c "$recipe"; then
    echo ">>> ok: $name"
  else
    echo "!!! failed: $name (continuing)"
    failed="$failed $name"
  fi
done

if [ -n "$failed" ]; then
  echo "=== finished with failures:$failed ==="
  # Non-zero so `docker compose run` surfaces that something needs attention,
  # but only after every recipe has been attempted.
  exit 1
fi
echo "=== all recipes ingested ==="
