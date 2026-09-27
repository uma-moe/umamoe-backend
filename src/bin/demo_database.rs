//! Initialize only the disposable Compose database; never reads DATABASE_URL or .env.
#![allow(non_snake_case)]

use anyhow::{ensure, Context};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--token") {
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "sub": "00000000-0000-4000-8000-000000000001",
            "iat": now,
            "exp": now + 7 * 24 * 3600,
        });
        println!(
            "{}",
            jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &jsonwebtoken::EncodingKey::from_secret(
                    b"local-demo-jwt-secret-not-for-production"
                ),
            )?
        );
        return Ok(());
    }
    let url = std::env::var("DEMO_DATABASE_URL").context("DEMO_DATABASE_URL must be set")?;
    validateDemoUrl(&url)?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(84629301)")
        .execute(&mut *tx)
        .await?;
    let initialized: bool =
        sqlx::query_scalar("SELECT to_regclass('public.demo_seed') IS NOT NULL")
            .fetch_one(&mut *tx)
            .await?;
    if !initialized {
        let empty: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public')",
        )
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            empty,
            "refusing to bootstrap a non-empty database without the demo marker"
        );
        sqlx::raw_sql(include_str!("../../deploy/demo/schema.sql"))
            .execute(&mut *tx)
            .await?;
        sqlx::query("CREATE TABLE demo_seed (id boolean PRIMARY KEY DEFAULT true CHECK (id), seeded_at timestamptz)")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO demo_seed (id) VALUES (true)")
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    let mut tx = pool.begin().await?;
    let seeded: bool = sqlx::query_scalar(
        "SELECT seeded_at IS NOT NULL FROM demo_seed WHERE id = true FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    // Search uses this legacy column, which is absent from the backend migrations.
    // Apply to existing demos too, without reseeding developer edits.
    sqlx::query(
        "ALTER TABLE inheritance ADD COLUMN IF NOT EXISTS scenario_id integer NOT NULL DEFAULT 0",
    )
    .execute(&mut *tx)
    .await?;
    if !seeded {
        sqlx::raw_sql(include_str!("../../deploy/demo/seed.sql"))
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE demo_seed SET seeded_at = now() WHERE id = true")
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    println!("Demo database ready; existing developer data is preserved.\nDemo trainer: 900000000001; API key: uma_demo_key_001");
    pool.close().await;
    Ok(())
}

fn validateDemoUrl(url: &str) -> anyhow::Result<()> {
    let parsed = url::Url::parse(url)?;
    ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("postgres" | "127.0.0.1" | "localhost")
            )
            && parsed.path() == "/umamoe_demo"
            && parsed.username() == "umamoe_demo"
            && parsed.query().is_none(),
        "demo setup requires the local umamoe_demo database and role, without URL query overrides"
    );
    Ok(())
}

#[test]
fn restrictDemoDatabaseTargets() {
    for host in ["postgres", "localhost", "127.0.0.1:55432"] {
        assert!(validateDemoUrl(&format!(
            "postgresql://umamoe_demo:umamoe_demo@{host}/umamoe_demo"
        ))
        .is_ok());
    }
    for url in [
        "postgresql://umamoe_demo@db.example.com/umamoe_demo",
        "postgresql://admin@localhost/umamoe_demo",
        "postgresql://umamoe_demo@localhost/production",
        "postgresql://umamoe_demo@localhost/umamoe_demo?host=db.example.com",
        "https://umamoe_demo@localhost/umamoe_demo",
    ] {
        assert!(
            validateDemoUrl(url).is_err(),
            "accepted unsafe demo URL: {url}"
        );
    }
}
