use centaurus::{error::Result, eyre::ContextCompat};
use entity::{cache_upload, cache_upload_part};
use sea_orm::{ActiveValue::Set, IntoActiveModel, prelude::*, sqlx::types::chrono::Utc};

use crate::storage::UploadPart;

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

  pub async fn chunk(&self, id: Uuid, size: i64, start: i64) -> Result<cache_upload_part::Model> {
    let model = cache_upload_part::ActiveModel {
      cache_upload: Set(id),
      id: sea_orm::ActiveValue::NotSet,
      size: Set(size),
      part_number: Set(0),
      e_tag: Set(None),
      start_byte: Set(start),
    };

    let model = model.insert(self.db).await?;

    let part_number = model.id % 10000;

    let mut model = model.into_active_model();
    model.part_number = Set(part_number);
    let model = model.update(self.db).await?;

    Ok(model)
  }

  pub async fn update_etag(&self, part_id: i32, e_tag: String) -> Result<()> {
    let mut model = cache_upload_part::Entity::find_by_id(part_id)
      .one(self.db)
      .await?
      .context("Cache upload part not found")?
      .into_active_model();

    model.e_tag = Set(Some(e_tag));

    model.update(self.db).await?;

    Ok(())
  }

  pub async fn find(&self, id: Uuid) -> Result<cache_upload::Model> {
    let entry = cache_upload::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache upload entry not found")?;

    Ok(entry)
  }

  pub async fn parts(&self, id: Uuid) -> Result<Vec<UploadPart>> {
    let parts = cache_upload_part::Entity::find()
      .filter(cache_upload_part::Column::CacheUpload.eq(id))
      .all(self.db)
      .await?;

    Ok(
      parts
        .into_iter()
        .map(|part| UploadPart {
          start_byte: part.start_byte,
          part_number: part.part_number,
          etag: part.e_tag,
          size: part.size,
        })
        .collect(),
    )
  }
}
