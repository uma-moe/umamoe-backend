# Uma.moe Backend

A high-performance Rust backend API for uma.moe, built with Axum and PostgreSQL. This service provides data management and search capabilities for Uma Musume game data including inheritance records, support cards, team stadium information, and trainer statistics.

## 🚀 Features

- **Search API**: Fast search across inheritance records and support cards
- **Inheritance System**: Track and manage character inheritance data with blue/pink/unique factors
- **Support Cards**: Store and retrieve support card information with limit break data
- **Team Stadium**: Character data for team competitions
- **Statistics**: Daily visitor tracking and usage analytics
- **Task Queue**: Background job processing system
- **Rate Limiting**: Built-in bot protection with Turnstile verification
- **Sharing**: URL shortening and content sharing functionality

## 🛠️ Tech Stack

- **Framework**: [Axum](https://github.com/tokio-rs/axum) (async web framework)
- **Database**: PostgreSQL with [SQLx](https://github.com/launchbadge/sqlx)
- **Runtime**: [Tokio](https://tokio.rs/) (async runtime)
- **Serialization**: [Serde](https://serde.rs/) (JSON handling)
- **Logging**: [Tracing](https://tracing.rs/) (structured logging)
- **Validation**: [Validator](https://github.com/Keats/validator) (input validation)
- **Security**: Tower middleware with CORS and rate limiting

## 📡 API Endpoints

### Core APIs
- `GET /api/health` - Health check and service status
- `GET /api/v3/search` - Search inheritance records and support cards
- `GET /api/stats` - Service statistics and metrics
- `GET /api/tasks` - Task queue management

### Data Management
- Inheritance record operations
- Support card data retrieval
- Team stadium character lookup
- Trainer information and statistics

## 🚦 Getting Started

Install and start Docker with Compose (Docker Desktop with Linux containers on
Windows). Then run the setup script from this checkout:

Windows, in PowerShell or Command Prompt:

```powershell
.\setup.cmd
```

Linux, macOS, or Git Bash:

```sh
sh ./setup.sh
```

The scripts check Docker, build the backend, start PostgreSQL and Redis, apply
migrations, seed the demo database, and wait for the backend to become healthy.
They print the local URL and login instructions. They also work when invoked by
path from another directory, and rerunning them preserves your database edits.

Only this repository is required. The scripts use public Docker images and build
the backend locally; they never clone private repositories or pull private images.
Internet access is needed on the first run for images and Rust dependencies. See
[optional services](#optional-services) for features that need separate access.

The equivalent manual command is:

```bash
docker compose -f compose.local.yml up --build -d --wait
```

The first build compiles Rust and can take several minutes. Open
[health](http://localhost:3001/api/health),
[search](http://localhost:3001/api/v3/search), or
[a demo profile](http://localhost:3001/api/v4/user/profile/900000000001).
No `.env`, database dump, OAuth credentials, or sibling repositories are needed.

Compose starts PostgreSQL 18 and Redis, initializes the database, applies the real
migration history, seeds it, and starts the backend after setup succeeds. Ports
bind to localhost. Its database URL and local secrets are explicit, so an existing
`.env` cannot redirect the demo into another database.

### Demo data and authentication

There are 100 linked trainers, circles, inheritance records, support cards, team
members, users, API keys, bookmarks, partner records, veterans, and planner states.
Other ordinary tables also receive 100 rows. Monthly fan history has 300 rows and
snapshots have 200 so historical pages work; migration and singleton metadata keep
their natural sizes. Names and identities are synthetic, and dates are relative
to the first setup. Materialized search, ranking, and statistics views are populated.

| Example | Value |
| --- | --- |
| Trainer | `900000000001` |
| Circle | `9000001` |
| User | `00000000-0000-4000-8000-000000000001` |
| API key | `uma_demo_key_001` (through `uma_demo_key_100`) |
| Shared planner | `/api/carat-planner/shared/demo00000001` |
| PostgreSQL | `localhost:55432`, database/user/password: `umamoe_demo` |

Browser proof protection is bypassed locally. For authenticated user endpoints,
print a seven-day token for Demo Developer 001:

```bash
docker compose -f compose.local.yml run --rm --no-deps demo-db --token
```

Use it as `Authorization: Bearer <token>` in an API client for `/api/auth/me`,
`/api/auth/bookmarks`, or `/api/carat-planner/state`. Public API clients can also
send `X-API-Key: uma_demo_key_001`. These credentials and bypass settings are for
this local stack only.

### Daily workflow

```bash
# Inspect startup failures or application logs
docker compose -f compose.local.yml logs --tail=100 demo-db backend

# Rebuild after changing Rust code
docker compose -f compose.local.yml up --build -d --wait

# Stop while keeping your database edits
docker compose -f compose.local.yml down

# Delete this demo database and start with fresh sample data
docker compose -f compose.local.yml down -v
docker compose -f compose.local.yml up --build -d --wait
```

Setup seeds only once and preserves edits on subsequent runs. New migrations still
apply. To load changes to the seed itself, reset the demo volume. Background
maintenance is disabled to keep the sample history stable; user writes are enabled.
Set `DEMO_BACKEND_PORT` or `DEMO_DATABASE_PORT` in your shell if the default ports
are occupied, and adjust client URLs accordingly.

Validate a fresh seed before making edits (requires no Rust installation):

```bash
docker compose -f compose.local.yml exec -T postgres psql -U umamoe_demo -d umamoe_demo -f /demo/check.sql
```

### Run Rust on the host

With current stable Rust installed, start only the disposable database and seed it:

```bash
docker compose -f compose.local.yml run --build --rm demo-db
```

Copy `.env.example` to `.env` in a new checkout, or merge its local settings into an
existing file. Then run `cargo run --locked`. If you previously started the full
stack, stop its backend first with `docker compose -f compose.local.yml stop backend`
to free port 3001. Native development uses the existing in-process cache fallback;
Redis is provided for the full Docker stack.

The setup binary is also available as `cargo run --locked --bin demo_database`.
It requires an explicitly exported `DEMO_DATABASE_URL`; it intentionally does not
read `.env` or `DATABASE_URL`. `cargo run --locked --bin demo_database -- --token`
prints the same local login token without connecting to a database.

### Optional services

The standalone stack covers this backend's PostgreSQL-backed APIs. The dedicated
affinity/search service, resource files, embeds, simulator, OAuth providers, and
game-data workers require their separate services or credentials. Queued demo
tasks remain pending until a worker is added.

### Collaborate across repositories

Check out the repositories beside each other under any parent directory (the
names below matter; the parent does not have to be called `git`):

```text
git/
  umamoe-backend/
  umamoe-frontend/
  umamoe-resources/
    master.mdb
  umamoe_db/
  umamoe-embeds/
```

Some repositories are private; each developer needs access to clone them. The
scripts use existing local checkouts and never clone or pull repositories.
Place the game's global `master.mdb` in `umamoe-resources/`; it is not included
in Git. JP planner data uses the resources repo's bundled catalogue, so a
separate `jp-master.mdb` is not required.

From `umamoe-backend`, run either:

```powershell
.\setup.cmd services
```

```sh
sh ./setup.sh services
```

These commands check the sibling paths, build all services, and wait for a
healthy stack: PostgreSQL, Redis, seeded backend, resources, search, frontend,
and embeds. No host database, `.env`, or embeds `local.env` is required. The
collaboration database and generated resources use their own Docker volumes;
rerunning setup preserves database edits. The scripts also work when invoked
by their full path from another directory.

Open the frontend at [localhost:4200](http://localhost:4200). Its development
server proxies `/api`, `/search`, `/resources`, and `/__embeds` to the containers
and reloads edits under the frontend's `src/`. Rerun setup after changing Rust,
frontend dependencies, or frontend build configuration. Direct service ports
are backend `3001`, search `3002`, resources `3004`, and embeds `3008`.

The equivalent Compose commands are:

```bash
docker compose -f compose.services.yml up --build -d --wait --wait-timeout 600
docker compose -f compose.services.yml logs --tail=100
docker compose -f compose.services.yml run --rm --no-deps demo-db --token
docker compose -f compose.services.yml down
```

Stop the standalone stack before switching to services mode because their
default ports overlap. Both modes use `DEMO_BACKEND_PORT` and
`DEMO_DATABASE_PORT`; services mode additionally supports `LOCAL_FRONTEND_PORT`,
`LOCAL_SEARCH_PORT`, `LOCAL_RESOURCES_PORT`, and `LOCAL_EMBEDS_PORT` in your shell.
The database URL and local credentials remain fixed to this disposable stack.
OAuth, ingest workers, and the private simulator still need separate setup.

## 🗄️ Database Schema

The application uses PostgreSQL with the following main tables:

- `inheritance` - Character inheritance data
- `support_card` - Support card information
- `team_stadium` - Team competition character data  
- `trainer` - Trainer profiles and statistics
- `daily_stats` - Usage analytics and visitor tracking
- `tasks` - Background job queue

## 🔧 Configuration

### CORS Configuration
- **Development**: Permissive CORS for all origins
- **Production**: Restricted to configured domains in `ALLOWED_ORIGINS`

### Rate Limiting
- Built-in rate limiting per account
- Turnstile verification middleware for bot protection

### Logging
- Structured logging with tracing
- Configurable log levels via environment filters
- SQL query logging (warnings only in production)

## 🚀 Deployment

The application is production-ready with:

- Automatic database migrations
- Health check endpoints
- Graceful error handling
- CORS configuration for web deployment
- Rate limiting and security middleware

### Docker

Build the production container image from the repository root:

```bash
docker build -t umamoe-backend:local .
```

Run production locally with an env file:

```bash
docker run --rm \
   --env-file .env \
   -e HOST=0.0.0.0 \
   -e PORT=3001 \
   -p 127.0.0.1:3001:3001 \
   umamoe-backend:local
```

Run a beta instance on the next port range:

```bash
docker run --rm \
   --env-file .env \
   -e HOST=0.0.0.0 \
   -e PORT=3101 \
   -e USER_WRITES_DISABLED=true \
   -p 127.0.0.1:3101:3101 \
   umamoe-backend:local
```

Set `USER_WRITES_DISABLED=true` for beta/read-only followers. This rejects user
and task mutations while still allowing backend-generated materialized views,
ranking refreshes, live rank cleanup, and suspicious-activity aggregates.

### GitHub Actions Deployment

The backend workflow at `.github/workflows/deploy-backend.yml` builds a Docker image, uploads it as an artifact, copies it to the server over SSH, and restarts one of two containers:

- `umamoe-backend` on port `3001`
- `umamoe-backend-beta` on port `3101`

Required repository or environment secrets:

- `DEPLOY_HOST`
- `DEPLOY_PORT` (optional, defaults to `22`)
- `DEPLOY_SSH_KEY`
- `DEPLOY_KNOWN_HOSTS`

On the server, create `/etc/umamoe-backend/prod.env` and `/etc/umamoe-backend/beta.env` using `deploy/backend.env.example` as the template. The deploy user must be able to run `docker` and write to `/tmp/umamoe-backend-images`.

## 🤝 Contributing

Follow [AGENTS.md](AGENTS.md) for the repository's design and Rust conventions.
Owned functions use lowerCamelCase; serialization names and external trait methods
keep their existing names.

The project uses one Cargo package:

- `src/main.rs` declares modules and starts the application through `src/app.rs`.
- `src/http/` assembles public and internal routes, CORS, and network guards.
- `src/handlers/` and `src/middleware/` handle HTTP requests and authentication.
- `src/database/` owns connections, migrations, and scheduled maintenance.
- `src/tasks.rs`, `src/cheat_analysis/`, and `src/shame/` provide reusable domain logic.
- `src/types/` contains data declarations, grouped by domain. Public records use
  ordinary modules; private declarations are included in their owning logic module
  so moving a declaration does not expose its fields. Implementations stay with the logic.
- `src/bin/` contains additional executables. `cargo run` still starts the backend.

Run the local checks without starting the server or connecting to a database:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --all-targets
python scripts/test_setup.py
```

Database-backed integration tests remain explicitly ignored and require disposable
databases. Do not use a production database for tests. Commit `Cargo.lock` so local
and Docker builds use the same dependency versions.

Maintenance scripts live in `scripts/` and are run from the repository root.
For example, use `python scripts/affinity.py` to generate affinity data and
`python scripts/affinity_test.py` for the existing local comparison tool.
The optional PostgreSQL configuration reference is `deploy/postgres.conf`.

The circle-rank analyzer accepts an exported TSV (optionally gzip-compressed) and
an output CSV:

```bash
cargo run --release --bin analyze_circle_rank_threshold_history -- INPUT.tsv.gz OUTPUT.csv
```

Keep generated analysis output under the ignored `reports/` directory. Existing
circle-rank charts and their CSV have been moved to `reports/circle-rank-thresholds/`.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🐎 About Uma.moe

Uma.moe is a community resource for Uma Musume Pretty Derby players, providing tools and data to help optimize character training and inheritance planning.
## Private simulator forwarding

The existing `/api/` proxy serves `POST /api/sim/replay`, `/api/sim/monte-carlo`,
`/api/sim/optimize`, `/api/sim/stamina` and `/api/sim/races/resimulate`. These routes require exactly
one granted `X-API-Key`; browser proofs, bearer tokens and cookies do not grant
simulator access. The backend checks revocation and records usage once, then
forwards to the private simulator. Read-only backends keep skipping usage writes.

`SIMULATOR_URL` is optional: unset or empty uses `http://192.168.100.1:3009`
in production and `http://192.168.100.1:3109` when `APP_ENV=beta`. The deployment
workflow sets `APP_ENV` automatically; outside it, the default is production.
An explicit `SIMULATOR_URL` takes precedence. No shared service key is required: the
simulator is reachable only through the private subnet and its firewall allows
the main server. User authorization stays in this backend.
Allowed user keys live in a text file, one key per line; blank lines and lines
starting with `#` are ignored. The backend reads the file on every simulator
request, so additions, removals and file replacements need no restart. The
previous `SIMULATOR_ALLOWED_KEYS` environment variable is no longer read.

On the main server, edit:

- Production: `/opt/umamoe-backend/simulator-production/allowed-keys.txt`
- Beta: `/opt/umamoe-backend/simulator-beta/allowed-keys.txt`

The workflow creates empty files only when absent and mounts each environment's
directory read-only at `/config/simulator`. It grants the container access through
the deployment user's group, with directory mode `750` and file mode `640`.
Mounting the directory lets editors replace the file atomically. Preserve the
file's owner and permissions when replacing it. Redeploy once to install this
mount and code; subsequent key edits take effect on the next request. Requests
already authorized may finish.

The default container path is `/config/simulator/allowed-keys.txt`; override it
with `SIMULATOR_ALLOWED_KEYS_FILE` for local runs or a different mounted path.
An empty file denies every key (403); unreadable, invalid or oversized files
return 503 without forwarding. Reads are bounded to 1 MiB. Keys must still be
valid and unrevoked in the backend database. Move any previous environment grant
list into the appropriate file before switching clients over.

Leaving the URL and file override unset uses the deployment defaults. A missing
allowed-key file returns HTTP 503 without forwarding; invalid URL configuration
fails startup. See `deploy/backend.env.example`.

The backend forwards the request content type; cookies and user credentials
are dropped, and redirects are rejected. Bodies are passed through unchanged;
responses are streamed with `Cache-Control: no-store`. Simulator overload (429)
and other application statuses are preserved. Connection failures return 502;
timeouts before response headers return 504. Mid-response failures abort the stream.
Requests are limited to 256 KiB, or 8 MiB for capture resimulation. The upstream
timeout is 120 seconds; existing Nginx body and timeout limits must accommodate
the requests you serve. No additional Nginx route or Cloudflare configuration
is needed, and the backend internal authentication listener stays unpublished.

Run `cargo test handlers::simulator::tests` for the local forwarding checks.
The ignored PostgreSQL integration check additionally verifies revocation,
request-size limits and single usage recording. It requires a disposable empty
database named `simulator_forwarding_test` on `127.0.0.1`, supplied as
`SIMULATOR_TEST_DATABASE_URL`, and runs with
`cargo test authenticatedForwardingRecordsUsageOnce -- --ignored`.
