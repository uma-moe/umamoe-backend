use crate::carat_planner_storage;
use sqlx::PgPool;
use tracing::{error, warn};

pub(crate) async fn run(pool: &PgPool, skip_migrations: bool) -> anyhow::Result<()> {
    // Run migrations with better error handling (can be disabled via env var)
    let ignore_migration_version_mismatch = std::env::var("IGNORE_MIGRATION_VERSION_MISMATCH")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);
    if skip_migrations {
        warn!("⚠️ Skipping migrations due to SKIP_MIGRATIONS=true");
    } else {
        warn!("🔄 Running database migrations...");

        let migrator = sqlx::migrate!("./migrations");

        baselineMigrationsFromEnv(pool, &migrator).await?;

        // Determine which migrations are already applied so we can log only the new ones.
        let applied: std::collections::HashSet<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success = true")
                .fetch_all(pool)
                .await
                .unwrap_or_default()
                .into_iter()
                .collect();

        let pending: Vec<_> = migrator
            .migrations
            .iter()
            .filter(|m| !applied.contains(&m.version))
            .collect();

        if pending.is_empty() {
            warn!("✅ All migrations already applied, nothing to do");
        } else {
            warn!("📋 {} migration(s) pending:", pending.len());
            for m in &pending {
                warn!("   ▶ {} — {}", m.version, m.description);
            }

            // Heartbeat task: polls _sqlx_migrations every few seconds so we
            // can log which migration just completed and how long the
            // currently-running one has been going. sqlx::Migrator::run
            // applies them sequentially in pending order, so we can derive
            // "currently running" as the first not-yet-applied pending one.
            let pending_for_watch: Vec<(i64, String)> = pending
                .iter()
                .map(|m| (m.version, m.description.to_string()))
                .collect();
            let pool_watch = pool.clone();
            let watch_handle = tokio::spawn(async move {
                let overall_start = std::time::Instant::now();
                let mut step_start = std::time::Instant::now();
                let mut current_idx: usize = 0;
                if let Some((v, d)) = pending_for_watch.first() {
                    warn!("   ▶ applying {} — {} ...", v, d);
                }
                let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(5));
                tick.tick().await; // discard immediate first tick
                loop {
                    tick.tick().await;
                    let applied_now: std::collections::HashSet<i64> = sqlx::query_scalar(
                        "SELECT version FROM _sqlx_migrations WHERE success = true",
                    )
                    .fetch_all(&pool_watch)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                    // Advance past any completions we missed since last tick.
                    while current_idx < pending_for_watch.len()
                        && applied_now.contains(&pending_for_watch[current_idx].0)
                    {
                        let (v, d) = &pending_for_watch[current_idx];
                        warn!(
                            "   ✅ {} — {} done in {:.1}s",
                            v,
                            d,
                            step_start.elapsed().as_secs_f64()
                        );
                        current_idx += 1;
                        step_start = std::time::Instant::now();
                        if let Some((nv, nd)) = pending_for_watch.get(current_idx) {
                            warn!("   ▶ applying {} — {} ...", nv, nd);
                        }
                    }

                    if current_idx >= pending_for_watch.len() {
                        break;
                    }

                    let (v, d) = &pending_for_watch[current_idx];
                    warn!(
                        "   ⏳ {} — {} still running ({:.0}s, {:.0}s total)",
                        v,
                        d,
                        step_start.elapsed().as_secs_f64(),
                        overall_start.elapsed().as_secs_f64()
                    );
                }
            });

            let run_result = migrator.run(pool).await;
            // Give the watcher one more tick to flush final completion lines,
            // then stop it regardless of outcome.
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            watch_handle.abort();

            match run_result {
                Ok(_) => warn!("✅ Migrations completed successfully"),
                Err(sqlx::migrate::MigrateError::VersionMismatch(version)) => {
                    if ignore_migration_version_mismatch && pending.is_empty() {
                        warn!(
                            "⚠️  Ignoring migration version mismatch for version {}",
                            version
                        );
                        warn!(
                            "Database migration history differs from this checkout, but startup will continue"
                        );
                    } else if ignore_migration_version_mismatch {
                        error!(
                            "⚠️  Migration version mismatch for version {} prevented {} pending migration(s) from running",
                            version,
                            pending.len()
                        );
                        error!(
                            "Refusing to continue because the application expects schema from pending migrations"
                        );
                        error!(
                            "Fix the migration mismatch or explicitly baseline the already-applied migrations before restarting"
                        );
                        return Err(anyhow::anyhow!(
                            "Migration version mismatch blocked pending migrations"
                        ));
                    } else {
                        error!("⚠️  Migration version mismatch: {}", version);
                        error!("Database has different migration state than expected");
                        error!("Set IGNORE_MIGRATION_VERSION_MISMATCH=true to continue startup when this is intentional");
                        error!("Consider resetting migrations: DROP TABLE _sqlx_migrations;");
                        return Err(anyhow::anyhow!("Migration version mismatch"));
                    }
                }
                Err(e) => {
                    error!("❌ Failed to run migrations: {}", e);
                    error!("Migration error details: {:?}", e);
                    return Err(anyhow::anyhow!("Migration failed: {}", e));
                }
            }
        }
    }

    if !skip_migrations {
        let migrated_plans = carat_planner_storage::backfillSparseV3(pool).await?;
        if migrated_plans > 0 {
            warn!(
                "✅ Compacted {} existing Carat Planner account state row(s) to sparse v3",
                migrated_plans
            );
        }
    }

    Ok(())
}

async fn baselineMigrationsFromEnv(
    pool: &PgPool,
    migrator: &sqlx::migrate::Migrator,
) -> anyhow::Result<()> {
    let baseline_all = std::env::var("BASELINE_MIGRATIONS")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let baseline_before = std::env::var("BASELINE_MIGRATIONS_BEFORE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.parse::<i64>())
        .transpose()?;
    let baseline_up_to = std::env::var("BASELINE_MIGRATIONS_UP_TO")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.parse::<i64>())
        .transpose()?;

    if !baseline_all && baseline_before.is_none() && baseline_up_to.is_none() {
        return Ok(());
    }

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // BASELINE_MIGRATIONS is a one-time adoption mechanism. Once a database
    // already has migration history, never let that broad flag silently mark
    // newly shipped migrations as applied without executing them.
    let existing_baseline_max = if baseline_all {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(version) FROM _sqlx_migrations WHERE success = true",
        )
        .fetch_one(pool)
        .await?
    } else {
        None
    };
    if let Some(version) = existing_baseline_max {
        warn!(
            "BASELINE_MIGRATIONS is already initialized through {}; newer migrations will execute normally",
            version
        );
    }

    let selected: Vec<_> = migrator
        .migrations
        .iter()
        .filter(|migration| {
            (baseline_all
                && existing_baseline_max
                    .map(|version| migration.version <= version)
                    .unwrap_or(true))
                || baseline_before.is_some_and(|version| migration.version < version)
                || baseline_up_to.is_some_and(|version| migration.version <= version)
        })
        .collect();

    if selected.is_empty() {
        warn!("⚠️ Migration baseline requested, but no migrations matched the configured range");
        return Ok(());
    }

    warn!(
        "⚠️ Baselining {} migration(s) into _sqlx_migrations without executing them",
        selected.len()
    );
    warn!("⚠️ Use this only when the database schema/data already has those migrations applied");

    for migration in selected {
        sqlx::query(
            r#"
            INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
            VALUES ($1, $2, TRUE, $3, 0)
            ON CONFLICT (version) DO UPDATE SET
                description = EXCLUDED.description,
                success = TRUE,
                checksum = EXCLUDED.checksum,
                execution_time = EXCLUDED.execution_time
            "#,
        )
        .bind(migration.version)
        .bind(&*migration.description)
        .bind(&*migration.checksum)
        .execute(pool)
        .await?;
    }

    Ok(())
}
