use centaurus::{
  Config, backend::config::BaseConfig, db::config::DBConfig, storage::StorageConfig,
};
use figment::{
  Figment,
  providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Deserialize, Serialize, Clone, Config)]
pub struct Config {
  #[base]
  #[serde(flatten)]
  pub base: BaseConfig,
  #[serde(flatten)]
  pub db: DBConfig,
  #[serde(flatten)]
  pub storage: StorageConfig,

  pub db_url: String,
  pub cache_secret: String,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      base: BaseConfig::default(),
      db: DBConfig::default(),
      storage: StorageConfig::default(),
      db_url: "".to_string(),
      cache_secret: "".to_string(),
    }
  }
}

impl Config {
  #[instrument]
  pub fn parse() -> Self {
    let config = Figment::new()
      .merge(Serialized::defaults(Self::default()))
      .merge(Env::raw().global());

    let mut config: Self = config.extract().expect("Failed to parse configuration");

    if config.db_url.is_empty() {
      panic!("Database URL is not set");
    }

    if config.db_url.starts_with("sqlite") {
      config.db.validate_sqlite();
    }

    config.storage.validate();

    if config.cache_secret.is_empty() {
      panic!("Cache secret is not set");
    }

    config
  }
}
