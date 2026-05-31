use centaurus::{
  bail,
  error::{ErrorReport, Result},
  eyre::{Context, ContextCompat},
};
use entity::{cache_cleanup, cache_entry, cache_entry_pending, cache_upload};
use migration::OnConflict;
use sea_orm::{
  ActiveValue::Set, IntoActiveModel, QueryOrder, TransactionTrait, prelude::*,
  sqlx::types::chrono::Utc,
};

use crate::auth::Auth;

pub struct CacheEntryTable<'db> {
  db: &'db DatabaseConnection,
}

impl<'db> CacheEntryTable<'db> {
  pub fn new(db: &'db DatabaseConnection) -> Self {
    Self { db }
  }

  pub async fn create_cache_pending(&self, upload: cache_upload::Model, size: i64) -> Result<i32> {
    let now = Utc::now().naive_utc();
    let entry = cache_entry_pending::ActiveModel {
      id: Set(upload.id),
      key: Set(upload.key),
      version: Set(upload.version),
      size: Set(size),
      created_at: Set(now),
      repo: Set(upload.repo),
      used_at: Set(now),
      write_isolation_key: Set(upload.write_isolation_key),
      file_id: Set(upload.file_id),
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
    self
      .db
      .transaction::<_, (), ErrorReport>(|tx| {
        Box::pin(async move {
          let Some(pending_model) = cache_entry_pending::Entity::find_by_id(id).one(tx).await?
          else {
            bail!("Cache pending entry not found");
          };

          let existing_entry = cache_entry::Entity::find()
            .filter(cache_entry::Column::Repo.eq(&pending_model.repo))
            .filter(cache_entry::Column::Version.eq(&pending_model.version))
            .filter(cache_entry::Column::WriteIsolationKey.eq(&pending_model.write_isolation_key))
            .filter(cache_entry::Column::Key.eq(&pending_model.key))
            .one(tx)
            .await?;

          let model = cache_entry::ActiveModel {
            id: Set(pending_model.id),
            key: Set(pending_model.key),
            version: Set(pending_model.version),
            size: Set(pending_model.size),
            created_at: Set(pending_model.created_at),
            repo: Set(pending_model.repo),
            used_at: Set(pending_model.used_at),
            write_isolation_key: Set(pending_model.write_isolation_key),
            file_id: Set(pending_model.file_id),
          };

          cache_entry::Entity::insert(model)
            .on_conflict(
              OnConflict::columns([
                cache_entry::Column::Repo,
                cache_entry::Column::Version,
                cache_entry::Column::WriteIsolationKey,
                cache_entry::Column::Key,
              ])
              .update_columns([
                cache_entry::Column::CreatedAt,
                cache_entry::Column::UsedAt,
                cache_entry::Column::Size,
                cache_entry::Column::FileId,
              ])
              .to_owned(),
            )
            .exec(tx)
            .await?;

          if let Some(existing_model) = existing_entry {
            cache_entry::Entity::delete_by_id(existing_model.id)
              .exec(tx)
              .await?;

            let model = cache_cleanup::ActiveModel {
              id: sea_orm::ActiveValue::NotSet,
              file_id: Set(existing_model.file_id),
            };
            model.insert(tx).await?;
          }

          cache_entry_pending::Entity::delete_by_id(id)
            .exec(tx)
            .await?;
          cache_upload::Entity::delete_by_id(id).exec(tx).await?;

          Ok(())
        })
      })
      .await
      .context("Failed to complete upload")?;

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

  pub async fn delete_pending_by_id(&self, id: i32) -> Result<()> {
    cache_entry_pending::Entity::delete_by_id(id)
      .exec(self.db)
      .await?;

    Ok(())
  }

  pub async fn find_incomplete_entries(
    &self,
    before: DateTime,
  ) -> Result<Vec<cache_entry_pending::Model>> {
    let entries = cache_entry_pending::Entity::find()
      .filter(cache_entry_pending::Column::CreatedAt.lt(before))
      .all(self.db)
      .await?;

    Ok(entries)
  }

  pub async fn find_unused_entries(&self, before: DateTime) -> Result<Vec<cache_entry::Model>> {
    let entries = cache_entry::Entity::find()
      .filter(cache_entry::Column::UsedAt.lt(before))
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
