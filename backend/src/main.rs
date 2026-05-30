use axum::{Extension, Router};
use centaurus::{
  backend::{
    endpoints::health,
    init::{listener_setup, run_app_connect_info},
    middleware::logging::logging,
  },
  db::init::init_db,
  logging::init_logging,
  storage::FileStorage,
  version_header,
};
#[cfg(debug_assertions)]
use dotenvy::dotenv;
use tracing::info;

use crate::config::Config;

mod auth;
mod config;
mod db;
mod storage;

#[tokio::main]
async fn main() {
  #[cfg(debug_assertions)]
  dotenv().ok();

  let config = Config::parse();
  init_logging(config.base.log_level);

  let listener = listener_setup(config.base.port).await;
  let mut app = state(api_router(), config).await;
  version_header!(app);

  info!("Starting application");
  run_app_connect_info(listener, app).await;
}

fn api_router() -> Router {
  Router::new().nest("/api", health::router())
}

async fn state(mut router: Router, config: Config) -> Router {
  let db = init_db::<migration::Migrator>(&config.db, &config.db_url).await;

  router = logging(router, |_| true);
  router = auth::state(router, &config);

  let storage = FileStorage::init(&config.storage)
    .await
    .expect("Failed to initialize storage");

  router
    .layer(Extension(db))
    .layer(Extension(storage))
    .layer(Extension(config))
}
