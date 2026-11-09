#!/usr/bin/env bash
set -e
superset db upgrade
superset fab create-admin \
  --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD" \
  --firstname Admin --lastname Lakehouse --email admin@example.com || true
superset init
exec /usr/bin/run-server.sh
