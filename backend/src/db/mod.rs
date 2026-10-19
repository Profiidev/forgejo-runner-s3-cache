use centaurus::db::init::Connection;

mod cache_entry;
mod cache_upload;

pub trait DBTrait {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_>;
  fn cache_entry(&self) -> cache_entry::CacheEntryTable<'_>;
}

impl DBTrait for Connection {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_> {
    cache_upload::CacheUploadTable::new(self)
  }

  fn cache_entry(&self) -> cache_entry::CacheEntryTable<'_> {
    cache_entry::CacheEntryTable::new(self)
  }
}

#[cfg(test)]
pub mod test {
  use centaurus::db::init::Connection;
  use chrono::{NaiveDateTime, Utc};
  use entity::{cache_entry, cache_upload};
  use migration::{Migrator, MigratorTrait};
  use sea_orm::{ActiveModelTrait, ActiveValue::NotSet, ConnectOptions, Database, Set};
  use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
  };
  use uuid::Uuid;

  /// In-memory SQLite with all migrations applied.
  ///
  /// `max_connections` is pinned to `1` so every query shares the same
  /// in-memory database (each `sqlite::memory:` connection is its own db).
  pub async fn test_db() -> Connection {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);

    let conn = Database::connect(options)
      .await
      .expect("Failed to connect to in-memory database");

    Migrator::up(&conn, None)
      .await
      .expect("Failed to run database migrations");

    Connection(conn)
  }

  /// Throwaway postgres container (same major version as production) with all
  /// migrations applied. Keep the container alive for the whole test.
  pub async fn test_pg_db() -> (Connection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
      .with_tag("18-alpine")
      .start()
      .await
      .expect("Failed to start postgres container");
    let url = format!(
      "postgres://postgres:postgres@{}:{}/postgres",
      container.get_host().await.unwrap(),
      container.get_host_port_ipv4(5432).await.unwrap()
    );

    let conn = Database::connect(url)
      .await
      .expect("Failed to connect to postgres");

    Migrator::up(&conn, None)
      .await
      .expect("Failed to run database migrations");

    (Connection(conn), container)
  }

  pub fn now() -> NaiveDateTime {
    Utc::now().naive_utc()
  }

  /// Inserts a `cache_upload` row created at `created_at`.
  pub async fn insert_upload(
    db: &Connection,
    repo: &str,
    key: &str,
    version: &str,
    write_isolation_key: &str,
    created_at: NaiveDateTime,
  ) -> cache_upload::Model {
    cache_upload::ActiveModel {
      id: NotSet,
      repo: Set(repo.into()),
      key: Set(key.into()),
      version: Set(version.into()),
      write_isolation_key: Set(write_isolation_key.into()),
      size: Set(None),
      created_at: Set(created_at),
      s3_upload_id: Set(None),
      file_id: Set(Uuid::new_v4()),
    }
    .insert(&db.0)
    .await
    .expect("insert cache_upload")
  }

  /// Inserts a committed `cache_entry` row.
  pub async fn insert_entry(
    db: &Connection,
    repo: &str,
    key: &str,
    version: &str,
    write_isolation_key: &str,
    created_at: NaiveDateTime,
    used_at: NaiveDateTime,
  ) -> cache_entry::Model {
    cache_entry::ActiveModel {
      id: NotSet,
      repo: Set(repo.into()),
      key: Set(key.into()),
      version: Set(version.into()),
      write_isolation_key: Set(write_isolation_key.into()),
      size: Set(0),
      created_at: Set(created_at),
      used_at: Set(used_at),
      file_id: Set(Uuid::new_v4()),
    }
    .insert(&db.0)
    .await
    .expect("insert cache_entry")
  }

  /// Inserts a `cache_entry_pending` row with the given id.
  pub async fn insert_pending(
    db: &Connection,
    id: i32,
    key: &str,
    created_at: NaiveDateTime,
  ) -> entity::cache_entry_pending::Model {
    entity::cache_entry_pending::ActiveModel {
      id: Set(id),
      repo: Set("repo".into()),
      key: Set(key.into()),
      version: Set("v1".into()),
      write_isolation_key: Set("".into()),
      size: Set(0),
      created_at: Set(created_at),
      used_at: Set(created_at),
      file_id: Set(Uuid::new_v4()),
    }
    .insert(&db.0)
    .await
    .expect("insert cache_entry_pending")
  }
}
