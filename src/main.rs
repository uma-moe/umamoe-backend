#![allow(non_snake_case)]

mod app;
mod auth;
mod borrow_key;
mod cache;
mod carat_planner_storage;
mod cheat_analysis;
mod club_rank;
mod config;
mod database;
mod errors;
mod handlers;
mod http;
mod middleware;
mod notify;
mod redis_store;
mod serialization;
mod shame;
mod tasks;
mod types;

pub use types::app::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}
