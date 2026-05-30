use centaurus::{error::Result, eyre::ContextCompat};
use entity::{cache_entry, cache_upload};
use sea_orm::{ActiveValue::Set, IntoActiveModel, prelude::*, sqlx::types::chrono::Utc};

pub struct CacheEntryTable<'db> {
  db: &'db DatabaseConnection,
}

impl<'db> CacheEntryTable<'db> {
  pub fn new(db: &'db DatabaseConnection) -> Self {
    Self { db }
  }

  pub async fn create_cache(&self, upload: cache_upload::Model, size: i64) -> Result<Uuid> {
    let entry = cache_entry::ActiveModel {
      id: Set(upload.id),
      key: Set(upload.key),
      version: Set(upload.version),
      size: Set(size),
      created_at: Set(Utc::now().naive_utc()),
      repo: Set(upload.repo),
      used_at: Set(None),
      write_isolation_key: Set(upload.write_isolation_key),
      complete: Set(false),
    };

    let model = entry.insert(self.db).await?;

    Ok(model.id)
  }

  pub async fn find_by_id(&self, id: Uuid) -> Result<cache_entry::Model> {
    let model = cache_entry::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache entry not found")?;

    Ok(model)
  }

  pub async fn complete(&self, id: Uuid) -> Result<()> {
    let model = self.find_by_id(id).await?;

    let mut model = model.into_active_model();
    model.complete = Set(true);
    model.update(self.db).await?;

    Ok(())
  }
}
