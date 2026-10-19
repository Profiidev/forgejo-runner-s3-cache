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

    if !config.storage.use_s3() {
      panic!("S3 storage is not enabled");
    }

    if config.cache_secret.is_empty() {
      panic!("Cache secret is not set");
    }

    config
  }
}

#[cfg(test)]
mod test {
  use super::*;

  const S3_ENV: [(&str, &str); 5] = [
    ("S3_BUCKET", "cache"),
    ("S3_REGION", "us-east-1"),
    ("S3_HOST", "http://localhost:9090"),
    ("S3_ACCESS_KEY", "access"),
    ("S3_SECRET_KEY", "secret"),
  ];

  /// Reset the process env to a valid configuration. Only sound because
  /// nextest runs every test in its own process.
  fn valid_env() {
    unsafe {
      for name in ["DATABASE_MAX_CONNECTIONS", "DATABASE_MIN_CONNECTIONS"] {
        std::env::remove_var(name);
      }
      std::env::set_var("DB_URL", "sqlite::memory:");
      std::env::set_var("CACHE_SECRET", "secret");
      for (name, value) in S3_ENV {
        std::env::set_var(name, value);
      }
    }
  }

  fn set(name: &str, value: &str) {
    unsafe { std::env::set_var(name, value) }
  }

  #[test]
  fn parses_full_environment() {
    valid_env();
    set("PORT", "1234");

    let config = Config::parse();
    assert_eq!(config.db_url, "sqlite::memory:");
    assert_eq!(config.cache_secret, "secret");
    assert_eq!(config.base.port, 1234);
    assert!(config.storage.use_s3());
    assert_eq!(config.storage.s3_bucket.as_deref(), Some("cache"));
    assert_eq!(config.storage.s3_region.as_deref(), Some("us-east-1"));
    assert_eq!(
      config.storage.s3_host.as_deref(),
      Some("http://localhost:9090")
    );
    assert_eq!(config.storage.s3_access_key.as_deref(), Some("access"));
    assert_eq!(config.storage.s3_secret_key.as_deref(), Some("secret"));
  }

  #[test]
  fn sqlite_clamps_connection_pool_to_one() {
    valid_env();
    set("DATABASE_MAX_CONNECTIONS", "10");
    set("DATABASE_MIN_CONNECTIONS", "5");

    let config = Config::parse();
    assert_eq!(config.db.database_max_connections, 1);
    assert_eq!(config.db.database_min_connections, 1);
  }

  #[test]
  fn postgres_keeps_connection_pool() {
    valid_env();
    set("DB_URL", "postgres://localhost/cache");
    set("DATABASE_MAX_CONNECTIONS", "10");

    let config = Config::parse();
    assert_eq!(config.db.database_max_connections, 10);
  }

  #[test]
  #[should_panic(expected = "Database URL is not set")]
  fn empty_db_url_panics() {
    valid_env();
    set("DB_URL", "");
    Config::parse();
  }

  #[test]
  #[should_panic(expected = "S3 storage is not enabled")]
  fn missing_s3_config_panics() {
    valid_env();
    unsafe { std::env::remove_var("S3_HOST") };
    set("STORAGE_PATH", "/tmp/cache");
    Config::parse();
  }

  #[test]
  #[should_panic(expected = "STORAGE_PATH is not set and S3 config is incomplete")]
  fn missing_s3_and_storage_path_panics() {
    valid_env();
    unsafe {
      std::env::remove_var("S3_BUCKET");
      std::env::remove_var("STORAGE_PATH");
    }
    Config::parse();
  }

  #[test]
  #[should_panic(expected = "Cache secret is not set")]
  fn empty_cache_secret_panics() {
    valid_env();
    set("CACHE_SECRET", "");
    Config::parse();
  }
}
