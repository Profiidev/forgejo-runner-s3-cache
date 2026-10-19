use axum::{Extension, Router};
use centaurus::{
  backend::{
    endpoints::health,
    init::{listener_setup, run_app_connect_info},
    middleware::logging::logging,
  },
  db::init::init_db,
  storage::FileStorage,
  version_header,
};
use tokio::net::TcpListener;
use tracing::info;

pub use crate::config::Config;

mod auth;
mod config;
mod db;
mod gc;
mod pull;
mod push;
mod storage;

pub struct App {
  app: Router,
  listener: TcpListener,
}

impl App {
  pub async fn new() -> App {
    App::from_config(Config::parse()).await
  }

  pub async fn from_config(config: Config) -> App {
    let listener = listener_setup(config.base.port).await;
    let mut app = state(api_router(), config).await;
    version_header!(app);

    App { app, listener }
  }

  pub fn port(&self) -> u16 {
    self
      .listener
      .local_addr()
      .expect("Failed to read listener address")
      .port()
  }

  pub async fn run(self) {
    info!("Starting application");
    run_app_connect_info(self.listener, self.app).await;
  }
}

fn api_router() -> Router {
  Router::new().nest("/api", health::router()).nest(
    "/_apis/artifactcache",
    Router::new().merge(push::router()).merge(pull::router()),
  )
}

async fn state(mut router: Router, config: Config) -> Router {
  let db = init_db::<migration::Migrator>(&config.db, &config.db_url).await;

  router = logging(router, |_| true);
  router = auth::state(router, &config);

  let storage = FileStorage::init(&config.storage)
    .await
    .expect("Failed to initialize storage");

  router = gc::state(router, db.clone(), storage.clone());

  router
    .layer(Extension(db))
    .layer(Extension(storage))
    .layer(Extension(config))
}
