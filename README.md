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

### Prerequisites

- Rust 1.70+ (2021 edition)
- PostgreSQL 12+
- Environment variables configured

### Environment Variables

Create a `.env` file in the root directory:

```env
DATABASE_URL=postgresql://username:password@localhost/uma_db
HOST=127.0.0.1
PORT=3001
DEBUG_MODE=true
ALLOWED_ORIGINS=https://uma.moe,https://www.uma.moe
SKIP_MIGRATIONS=false
```

### Installation & Running

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd umamoe-backend
   ```

2. **Install dependencies**
   ```bash
   cargo build
   ```

3. **Set up the database**
   ```bash
   # The migrations will run automatically on startup
   # Or manually run: sqlx migrate run
   ```

4. **Start the server**
   ```bash
   cargo run
   ```

The server will start on `http://127.0.0.1:3001` by default.

## 🗄️ Database Schema

The application uses PostgreSQL with the following main tables:

- `inheritance_records` - Character inheritance data
- `support_card_records` - Support card information
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
   -p 127.0.0.1:3101:3101 \
   umamoe-backend:local
```

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

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🐎 About Uma.moe

Uma.moe is a community resource for Uma Musume Pretty Derby players, providing tools and data to help optimize character training and inheritance planning.