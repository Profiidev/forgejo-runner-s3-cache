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

  pub async fn reserve(
    &self,
    key: String,
    version: String,
    size: Option<i64>,
    repo: String,
    write_isolation_key: String,
  ) -> Result<i32> {
    let entry = cache_upload::ActiveModel {
      key: Set(key),
      version: Set(version),
      size: Set(size),
      created_at: Set(Utc::now().naive_utc()),
      repo: Set(repo),
      write_isolation_key: Set(write_isolation_key),
      id: sea_orm::ActiveValue::NotSet,
      s3_upload_id: sea_orm::ActiveValue::NotSet,
    };

    let model = entry.insert(self.db).await?;

    Ok(model.id)
  }

  pub async fn set_s3_upload_id(&self, id: i32, s3_upload_id: String) -> Result<()> {
    let mut model = cache_upload::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache upload entry not found")?
      .into_active_model();

    model.s3_upload_id = Set(Some(s3_upload_id));

    model.update(self.db).await?;

    Ok(())
  }

  pub async fn chunk(&self, id: i32, size: i64, start: i64) -> Result<cache_upload_part::Model> {
    let existing = cache_upload_part::Entity::find()
      .filter(cache_upload_part::Column::CacheUpload.eq(id))
      .filter(cache_upload_part::Column::StartByte.eq(start))
      .one(self.db)
      .await?;

    if let Some(model) = existing {
      let mut model = model.into_active_model();
      model.size = Set(size);
      let model = model.update(self.db).await?;
      return Ok(model);
    }

    let model = cache_upload_part::ActiveModel {
      cache_upload: Set(id),
      id: sea_orm::ActiveValue::NotSet,
      size: Set(size),
      part_number: Set(0),
      e_tag: Set(None),
      start_byte: Set(start),
    };

    let model = model.insert(self.db).await?;

    // S3 part number
    let part_number = (model.id % 10000) as i32 + 1;

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

  pub async fn find(&self, id: i32) -> Result<cache_upload::Model> {
    let entry = cache_upload::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache upload entry not found")?;

    Ok(entry)
  }

  pub async fn parts(&self, id: i32) -> Result<Vec<UploadPart>> {
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

  pub async fn refresh_created_at(&self, id: i32) -> Result<()> {
    let mut model = cache_upload::Entity::find_by_id(id)
      .one(self.db)
      .await?
      .context("Cache upload entry not found")?
      .into_active_model();

    model.created_at = Set(Utc::now().naive_utc());

    model.update(self.db).await?;

    Ok(())
  }

  pub async fn find_incomplete_uploads(
    &self,
    before: DateTime,
  ) -> Result<Vec<(cache_upload::Model, Vec<i32>)>> {
    let entries = cache_upload::Entity::find()
      .filter(cache_upload::Column::CreatedAt.lt(before))
      .find_with_related(cache_upload_part::Entity)
      .all(self.db)
      .await?
      .into_iter()
      .map(|(upload, parts)| (upload, parts.iter().map(|p| p.part_number).collect()))
      .collect();

    Ok(entries)
  }

  pub async fn delete(&self, id: i32) -> Result<()> {
    cache_upload::Entity::delete_by_id(id).exec(self.db).await?;

    Ok(())
  }
}
