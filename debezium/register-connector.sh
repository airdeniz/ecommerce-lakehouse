#!/bin/bash
set -euo pipefail

echo "Registering Debezium connector..."

# Wait until the Connect REST API is TRULY ready. Just establishing a connection
# is not enough (the port may be open while the service is not ready yet); we
# wait for /connectors to return 200. Otherwise the POST hits a 404 and the
# connector is silently not registered (seen on slow hosts like Apple Silicon).
until [ "$(curl -s -o /dev/null -w '%{http_code}' http://connect:8083/connectors)" = "200" ]; do
  echo "Connect service not ready, waiting..."
  sleep 5
done

# Register the connector and check the HTTP code (201 created / 200 ok).
# 409 = already exists (idempotent, fine). 500/503/000 are TRANSIENT: on a fresh
# 3-broker KRaft cluster the REST API can answer 200 while the internal RF=3
# topics don't yet have ISR>=2, so persisting the connector config fails with
# NOT_ENOUGH_REPLICAS. Retry those a few times before giving up. A 4xx (bad
# config) is permanent -> fail immediately, retrying it is pointless.
max_attempts=10
attempt=1
while :; do
  code=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
    -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://connect:8083/connectors \
    -d @/register-postgres.json)

  if [ "$code" = "201" ] || [ "$code" = "200" ]; then
    echo "Connector registered (HTTP $code)."
    break
  elif [ "$code" = "409" ]; then
    echo "Connector already registered (HTTP 409) - fine."
    break
  elif [ "$code" = "500" ] || [ "$code" = "503" ] || [ "$code" = "000" ]; then
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "ERROR: connector registration still failing after $attempt attempts (HTTP $code). Response:"
      cat /tmp/resp.json
      exit 1
    fi
    echo "Transient error (HTTP $code), attempt $attempt/$max_attempts - retrying in 5s..."
    attempt=$((attempt + 1))
    sleep 5
  else
    echo "ERROR: connector registration failed with a permanent error (HTTP $code). Response:"
    cat /tmp/resp.json
    exit 1
  fi
done
