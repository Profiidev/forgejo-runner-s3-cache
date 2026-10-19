use backend::{App, Config};
use centaurus::logging::init_logging;
#[cfg(debug_assertions)]
use dotenvy::dotenv;

#[tokio::main]
async fn main() {
  #[cfg(debug_assertions)]
  dotenv().ok();

  let config = Config::parse();
  init_logging(config.base.log_level);

  App::from_config(config).await.run().await;
}
