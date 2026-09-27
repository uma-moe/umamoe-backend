#!/bin/sh
set -eu

cd -- "$(dirname -- "$0")"

if ! command -v docker >/dev/null 2>&1; then
    echo "Install Docker with the Compose plugin, then run this script again." >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose is required. Install or update Docker Compose, then retry." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "Start Docker Desktop or the Docker daemon, then run this script again." >&2
    exit 1
fi

echo "Starting the standalone demo. The first Rust build can take several minutes."
if ! docker compose -f compose.local.yml up --build -d --wait --wait-timeout 180; then
    echo "Setup failed. Check: docker compose -f compose.local.yml logs --tail=100" >&2
    exit 1
fi

address=$(docker compose -f compose.local.yml port backend 3001)
printf '\nDemo ready: http://%s/api/health\n' "$address"
echo "API key: uma_demo_key_001"
echo "Local login token: docker compose -f compose.local.yml run --rm --no-deps demo-db --token"
echo "Stop: docker compose -f compose.local.yml down"
