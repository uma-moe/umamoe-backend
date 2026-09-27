#!/bin/sh
set -eu

cd -- "$(dirname -- "$0")"

compose_file=compose.local.yml
wait_timeout=180
mode=${1:-}
frontend_ready=false
if [ "$#" -gt 1 ]; then
    echo "Usage: sh setup.sh [services|bootstrap]" >&2
    exit 1
fi
case "${1:-}" in
    '') ;;
    services)
        compose_file=compose.services.yml
        wait_timeout=600
        frontend_ready=true
        for file in umamoe-resources/Dockerfile umamoe_db/Dockerfile umamoe-embeds/Dockerfile umamoe-frontend/package-lock.json; do
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
    bootstrap)
        compose_file=compose.services.yml
        wait_timeout=600
        ;;
    *) echo "Usage: sh setup.sh [services|bootstrap]" >&2; exit 1 ;;
esac

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

set --
if [ "$mode" = bootstrap ]; then
    if ! command -v git >/dev/null 2>&1; then
        echo "Install Git, then run bootstrap again." >&2
        exit 1
    fi
    # Use saved credentials; inaccessible private repositories must not prompt.
    export GIT_TERMINAL_PROMPT=0 GCM_INTERACTIVE=never

    syncRepo() {
        repo=../$1
        if [ ! -e "$repo" ] && [ ! -L "$repo" ]; then
            git -c credential.interactive=false clone "https://github.com/uma-moe/$2.git" "$repo" ||
                echo "Could not clone $1 (access or network unavailable); continuing." >&2
            return
        fi
        if [ ! -e "$repo/.git" ]; then
            echo "Keeping $repo: existing directory is not a Git checkout."
            return
        fi
        if ! changes=$(git -C "$repo" status --porcelain); then
            echo "Keeping $repo: could not read Git status." >&2
            return
        fi
        if [ -n "$changes" ]; then
            echo "Keeping $repo: local changes present."
            return
        fi
        if ! git -C "$repo" rev-parse --verify '@{upstream}' >/dev/null 2>&1; then
            echo "Keeping $repo: current branch has no upstream."
            return
        fi
        git -c credential.interactive=false -C "$repo" pull --ff-only --no-rebase --no-autostash ||
            echo "Keeping $repo: update unavailable or requires a merge; using the existing checkout." >&2
    }

    syncRepo umamoe-resources umamoe-resources
    syncRepo umamoe_db umamoe-search
    syncRepo umamoe-frontend umamoe-frontend
    syncRepo umamoe-embeds umamoe-embeds
    docker compose -f "$compose_file" pull postgres redis ||
        echo "Image download failed; trying cached images." >&2
    set -- backend
fi

echo "Starting $compose_file. The first build can take several minutes."
if ! docker compose -f "$compose_file" up --build -d --wait --wait-timeout "$wait_timeout" "$@"; then
    echo "Setup failed. Check: docker compose -f $compose_file logs --tail=100" >&2
    exit 1
fi

if [ "$mode" = bootstrap ]; then
    started="postgres redis backend"
    startService() {
        if docker compose -f "$compose_file" up --build -d --wait --wait-timeout "$wait_timeout" --no-deps "$1"; then
            started="$started $1"
            return 0
        fi
        echo "Could not start $1; continuing. Check: docker compose -f $compose_file logs --tail=100 $1" >&2
        return 1
    }

    resources_ready=false
    if [ -f ../umamoe-resources/Dockerfile ] && [ -f ../umamoe-resources/master.mdb ]; then
        if startService resources; then resources_ready=true; fi
    else
        echo "Skipping resources: need ../umamoe-resources/Dockerfile and master.mdb."
    fi
    if [ "$resources_ready" = true ] && [ -f ../umamoe_db/Dockerfile ]; then
        startService search || :
    else
        echo "Skipping search: need ../umamoe_db/Dockerfile and healthy resources."
    fi
    if [ -f ../umamoe-frontend/package-lock.json ]; then
        if startService frontend; then frontend_ready=true; fi
    else
        echo "Skipping frontend: need ../umamoe-frontend/package-lock.json."
    fi
    if [ "$frontend_ready" = true ] && [ -f ../umamoe-embeds/Dockerfile ]; then
        startService embeds || :
    else
        echo "Skipping embeds: need ../umamoe-embeds/Dockerfile and a healthy frontend."
    fi
    echo "Bootstrap started: $started. Skipped or failed services can be retried by rerunning bootstrap."
fi

address=$(docker compose -f "$compose_file" port backend 3001)
printf '\nDemo ready: http://%s/api/health\n' "$address"
if [ "$frontend_ready" = true ]; then
    address=$(docker compose -f "$compose_file" port frontend 4200)
    printf 'Frontend: http://%s\n' "$address"
fi
echo "API key: uma_demo_key_001"
echo "Local login token: docker compose -f $compose_file run --rm --no-deps demo-db --token"
echo "Stop: docker compose -f $compose_file down"
