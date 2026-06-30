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
# 409 = already exists (idempotent, fine). Any other code is an ERROR -> exit.
code=$(curl -s -o /tmp/resp.json -w '%{http_code}' \
  -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://connect:8083/connectors \
  -d @/register-postgres.json)

if [ "$code" = "201" ] || [ "$code" = "200" ]; then
  echo "Connector registered (HTTP $code)."
elif [ "$code" = "409" ]; then
  echo "Connector already registered (HTTP 409) - fine."
else
  echo "ERROR: connector registration failed (HTTP $code). Response:"
  cat /tmp/resp.json
  exit 1
fi
