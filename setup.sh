#!/bin/sh
set -eu

cd -- "$(dirname -- "$0")"

compose_file=compose.local.yml
wait_timeout=180
case "${1:-}" in
    '') ;;
    services)
        compose_file=compose.services.yml
        wait_timeout=600
        for file in umamoe-resources/Dockerfile umamoe_db/Dockerfile umamoe-embeds/Dockerfile umamoe-frontend/package-lock.json umamoe-frontend/angular.json; do
            if [ ! -f "../$file" ]; then
                echo "Missing ../$file. Check out the required repo beside umamoe-backend (private repos require access)." >&2
                exit 1
            fi
        done
        if [ ! -f ../umamoe-resources/master.mdb ]; then
            echo "Place the game's master.mdb in ../umamoe-resources/master.mdb before starting services." >&2
            exit 1
        fi
        ;;
    *) echo "Usage: sh setup.sh [services]" >&2; exit 1 ;;
esac
if [ "$#" -gt 1 ]; then
    echo "Usage: sh setup.sh [services]" >&2
    exit 1
fi

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

echo "Starting $compose_file. The first build can take several minutes."
if ! docker compose -f "$compose_file" up --build -d --wait --wait-timeout "$wait_timeout"; then
    echo "Setup failed. Check: docker compose -f $compose_file logs --tail=100" >&2
    exit 1
fi

address=$(docker compose -f "$compose_file" port backend 3001)
printf '\nDemo ready: http://%s/api/health\n' "$address"
if [ "$compose_file" = compose.services.yml ]; then
    address=$(docker compose -f "$compose_file" port frontend 4200)
    printf 'Frontend: http://%s\n' "$address"
fi
echo "API key: uma_demo_key_001"
echo "Local login token: docker compose -f $compose_file run --rm --no-deps demo-db --token"
echo "Stop: docker compose -f $compose_file down"
