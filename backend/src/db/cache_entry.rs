use centaurus::{error::Result, eyre::ContextCompat};
use entity::{cache_entry, cache_upload};
use sea_orm::{
  ActiveValue::Set, IntoActiveModel, QueryOrder, prelude::*, sqlx::types::chrono::Utc,
};

use crate::auth::Auth;

pub struct CacheEntryTable<'db> {
  db: &'db DatabaseConnection,
}

impl<'db> CacheEntryTable<'db> {
  pub fn new(db: &'db DatabaseConnection) -> Self {
    Self { db }
  }

  pub async fn create_cache(&self, upload: cache_upload::Model, size: i64) -> Result<i32> {
    let now = Utc::now().naive_utc();
    let entry = cache_entry::ActiveModel {
      id: Set(upload.id),
      key: Set(upload.key),
      version: Set(upload.version),
      size: Set(size),
      created_at: Set(now),
      repo: Set(upload.repo),
      used_at: Set(now),
      write_isolation_key: Set(upload.write_isolation_key),
      complete: Set(false),
    };

    let model = entry.insert(self.db).await?;

    Ok(model.id)
  }

  pub async fn find_by_id(&self, id: i32) -> Result<Option<cache_entry::Model>> {
    let model = cache_entry::Entity::find_by_id(id).one(self.db).await?;

    Ok(model)
  }

  async fn find(&self, id: i32) -> Result<cache_entry::Model> {
    let model = cache_entry::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache entry not found")?;

    Ok(model)
  }

  pub async fn complete(&self, id: i32) -> Result<()> {
    let model = self.find(id).await?;

    let mut model = model.into_active_model();
    model.complete = Set(true);
    model.update(self.db).await?;

    cache_upload::Entity::delete_by_id(id).exec(self.db).await?;

    Ok(())
  }

  pub async fn update_used_at(&self, id: i32) -> Result<()> {
    let model = self.find(id).await?;

    let mut model = model.into_active_model();
    model.used_at = Set(Utc::now().naive_utc());
    model.update(self.db).await?;

    Ok(())
  }

  pub async fn find_entry(
    &self,
    keys: &[String],
    version: String,
    auth: Auth,
  ) -> Result<Option<cache_entry::Model>> {
    if let Some(entry) = self
      .find_cache(keys, &version, &auth.repo, &auth.write_isolation_key)
      .await?
    {
      return Ok(Some(entry));
    }

    if !auth.write_isolation_key.is_empty()
      && let Some(entry) = self.find_cache(keys, &version, &auth.repo, "").await?
    {
      return Ok(Some(entry));
    }

    Ok(None)
  }

  async fn find_cache(
    &self,
    keys: &[String],
    version: &str,
    repo: &str,
    write_isolation_key: &str,
  ) -> Result<Option<cache_entry::Model>> {
    for key in keys {
      let base_query = cache_entry::Entity::find()
        .filter(cache_entry::Column::Repo.eq(repo))
        .filter(cache_entry::Column::Version.eq(version))
        .filter(cache_entry::Column::WriteIsolationKey.eq(write_isolation_key))
        .filter(cache_entry::Column::Complete.eq(true))
        .order_by_desc(cache_entry::Column::CreatedAt);

      let exact_match = base_query
        .clone()
        .filter(cache_entry::Column::Key.eq(key))
        .one(self.db)
        .await?;

      if let Some(entry) = exact_match {
        return Ok(Some(entry));
      }

      let escaped_key = escape_like(key);
      let wildcard_prefix = format!("{}%", escaped_key);
      let prefix_match = base_query
        .clone()
        .filter(cache_entry::Column::Key.like(wildcard_prefix))
        .one(self.db)
        .await?;

      if let Some(entry) = prefix_match {
        return Ok(Some(entry));
      }
    }

    Ok(None)
  }

  pub async fn delete_by_id(&self, id: i32) -> Result<()> {
    cache_entry::Entity::delete_by_id(id).exec(self.db).await?;

    Ok(())
  }

  pub async fn clean_incomplete_entries(&self, before: DateTime) -> Result<()> {
    cache_entry::Entity::delete_many()
      .filter(cache_entry::Column::Complete.eq(false))
      .filter(cache_entry::Column::CreatedAt.lt(before))
      .exec(self.db)
      .await?;

    Ok(())
  }

  pub async fn find_unused_entries(&self, before: DateTime) -> Result<Vec<cache_entry::Model>> {
    let entries = cache_entry::Entity::find()
      .filter(cache_entry::Column::UsedAt.lt(before))
      .filter(cache_entry::Column::Complete.eq(true))
      .all(self.db)
      .await?;

    Ok(entries)
  }

  pub async fn find_entries_before(&self, before: DateTime) -> Result<Vec<cache_entry::Model>> {
    let entries = cache_entry::Entity::find()
      .filter(cache_entry::Column::CreatedAt.lt(before))
      .all(self.db)
      .await?;

    Ok(entries)
  }
}

fn escape_like(s: &str) -> String {
  s.replace('\\', "\\\\")
    .replace('%', "\\%")
    .replace('_', "\\_")
}
