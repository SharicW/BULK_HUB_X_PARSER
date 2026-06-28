#!/bin/sh
set -e

echo "Running DB migration..."
node migrate.js

echo "Starting parser loop..."
while true; do
  echo "[$(date)] ingest-new..."
  node parser.js ingest-new || true

  echo "[$(date)] refresh-metrics..."
  node parser.js refresh-metrics || true

  echo "[$(date)] refresh-users..."
  node parser.js refresh-users || true

  echo "[$(date)] sync-members..."
  node parser.js sync-members || true

  echo "[$(date)] Sleeping 1 hour..."
  sleep 3600
done
