use centaurus::error::Result;
use entity::cache_upload;
use sea_orm::{ActiveValue::Set, prelude::*, sqlx::types::chrono::Utc};

pub struct CacheUploadTable<'db> {
  db: &'db DatabaseConnection,
}

impl<'db> CacheUploadTable<'db> {
  pub fn new(db: &'db DatabaseConnection) -> Self {
    Self { db }
  }

  #[allow(clippy::too_many_arguments)]
  pub async fn reserve(
    &self,
    id: Uuid,
    key: String,
    version: String,
    size: Option<i64>,
    repo: String,
    write_isolation_key: String,
    s3_upload_id: Option<String>,
  ) -> Result<()> {
    let entry = cache_upload::ActiveModel {
      id: Set(id),
      key: Set(key),
      version: Set(version),
      size: Set(size),
      created_at: Set(Utc::now().naive_utc()),
      repo: Set(repo),
      write_isolation_key: Set(write_isolation_key),
      s3_upload_id: Set(s3_upload_id),
    };

    entry.insert(self.db).await?;

    Ok(())
  }
}
