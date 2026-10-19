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
  ) -> Result<(i32, Uuid)> {
    let entry = cache_upload::ActiveModel {
      key: Set(key),
      version: Set(version),
      size: Set(size),
      created_at: Set(Utc::now().naive_utc()),
      repo: Set(repo),
      write_isolation_key: Set(write_isolation_key),
      id: sea_orm::ActiveValue::NotSet,
      s3_upload_id: sea_orm::ActiveValue::NotSet,
      file_id: Set(Uuid::new_v4()),
    };

    let model = entry.insert(self.db).await?;

    Ok((model.id, model.file_id))
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

  pub async fn chunk(
    &self,
    id: i32,
    size: i64,
    start: i64,
    part_number: i32,
  ) -> Result<cache_upload_part::Model> {
    let existing = cache_upload_part::Entity::find()
      .filter(cache_upload_part::Column::CacheUpload.eq(id))
      .filter(cache_upload_part::Column::StartByte.eq(start))
      .one(self.db)
      .await?;

    if let Some(model) = existing {
      let mut model = model.into_active_model();
      model.size = Set(size);
      model.part_number = Set(part_number);
      let model = model.update(self.db).await?;
      return Ok(model);
    }

    let model = cache_upload_part::ActiveModel {
      cache_upload: Set(id),
      id: sea_orm::ActiveValue::NotSet,
      size: Set(size),
      part_number: Set(part_number),
      e_tag: Set(None),
      start_byte: Set(start),
    };

    let model = model.insert(self.db).await?;

    Ok(model)
  }

  /// Size of any part of the upload other than the one at `start`.
  pub async fn chunk_size(&self, id: i32, start: i64) -> Result<Option<i64>> {
    let part = cache_upload_part::Entity::find()
      .filter(cache_upload_part::Column::CacheUpload.eq(id))
      .filter(cache_upload_part::Column::StartByte.ne(start))
      .one(self.db)
      .await?;

    Ok(part.map(|part| part.size))
  }

  pub async fn set_part(&self, part_id: i32, part_number: i32, e_tag: String) -> Result<()> {
    let mut model = cache_upload_part::Entity::find_by_id(part_id)
      .one(self.db)
      .await?
      .context("Cache upload part not found")?
      .into_active_model();

    model.part_number = Set(part_number);
    model.e_tag = Set(Some(e_tag));

    model.update(self.db).await?;

    Ok(())
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
          id: part.id,
          part_number: part.part_number,
          start_byte: part.start_byte,
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
  ) -> Result<Vec<cache_upload::Model>> {
    let entries = cache_upload::Entity::find()
      .filter(cache_upload::Column::CreatedAt.lt(before))
      .all(self.db)
      .await?;

    Ok(entries)
  }

  pub async fn delete(&self, id: i32) -> Result<()> {
    cache_upload::Entity::delete_by_id(id).exec(self.db).await?;

    Ok(())
  }
}

#[cfg(test)]
mod test {
  use chrono::Duration;
  use entity::cache_upload_part;
  use sea_orm::{EntityTrait, PaginatorTrait};

  use crate::db::{
    DBTrait,
    test::{insert_upload, now, test_db},
  };

  async fn reserve(db: &centaurus::db::init::Connection) -> i32 {
    db.cache_upload()
      .reserve(
        "key".into(),
        "v1".into(),
        Some(42),
        "repo".into(),
        "wik".into(),
      )
      .await
      .unwrap()
      .0
  }

  // ---- reserve ----

  #[tokio::test]
  async fn reserve_stores_all_fields_without_s3_upload_id() {
    let db = test_db().await;
    let before = now();

    let (id, file_id) = db
      .cache_upload()
      .reserve(
        "key".into(),
        "v1".into(),
        Some(42),
        "repo".into(),
        "wik".into(),
      )
      .await
      .unwrap();

    let row = db.cache_upload().find(id).await.unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.file_id, file_id);
    assert_eq!(row.key, "key");
    assert_eq!(row.version, "v1");
    assert_eq!(row.size, Some(42));
    assert_eq!(row.repo, "repo");
    assert_eq!(row.write_isolation_key, "wik");
    assert_eq!(row.s3_upload_id, None);
    assert!(row.created_at >= before && row.created_at <= now());
  }

  #[tokio::test]
  async fn reserve_generates_distinct_ids_and_file_ids() {
    let db = test_db().await;
    let a = db
      .cache_upload()
      .reserve("k".into(), "v".into(), None, "r".into(), "".into())
      .await
      .unwrap();
    let b = db
      .cache_upload()
      .reserve("k".into(), "v".into(), None, "r".into(), "".into())
      .await
      .unwrap();

    assert_ne!(a.0, b.0);
    assert_ne!(a.1, b.1);
    assert_eq!(db.cache_upload().find(a.0).await.unwrap().size, None);
  }

  // ---- set_s3_upload_id ----

  #[tokio::test]
  async fn set_s3_upload_id_stores_value() {
    let db = test_db().await;
    let id = reserve(&db).await;

    db.cache_upload()
      .set_s3_upload_id(id, "s3-id".into())
      .await
      .unwrap();

    let row = db.cache_upload().find(id).await.unwrap();
    assert_eq!(row.s3_upload_id.as_deref(), Some("s3-id"));
  }

  #[tokio::test]
  async fn set_s3_upload_id_missing_upload_errors() {
    let db = test_db().await;
    let err = db
      .cache_upload()
      .set_s3_upload_id(999, "s3-id".into())
      .await
      .unwrap_err();
    assert_eq!(err.error.to_string(), "Cache upload entry not found");
  }

  // ---- chunk ----

  #[tokio::test]
  async fn chunk_inserts_new_part() {
    let db = test_db().await;
    let id = reserve(&db).await;

    let part = db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();

    assert_eq!(part.cache_upload, id);
    assert_eq!(part.size, 100);
    assert_eq!(part.start_byte, 0);
    assert_eq!(part.part_number, 1);
    assert_eq!(part.e_tag, None);
  }

  #[tokio::test]
  async fn chunk_with_same_start_byte_updates_size_and_part_number_without_new_row() {
    let db = test_db().await;
    let id = reserve(&db).await;

    let first = db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();
    let second = db.cache_upload().chunk(id, 50, 0, 7).await.unwrap();

    assert_eq!(second.id, first.id);
    assert_eq!(second.size, 50);
    assert_eq!(second.part_number, 7);
    let count = cache_upload_part::Entity::find()
      .count(&db.0)
      .await
      .unwrap();
    assert_eq!(count, 1);
  }

  #[tokio::test]
  async fn chunk_same_start_byte_on_other_upload_is_separate_part() {
    let db = test_db().await;
    let a = reserve(&db).await;
    let b = reserve(&db).await;

    let pa = db.cache_upload().chunk(a, 100, 0, 1).await.unwrap();
    let pb = db.cache_upload().chunk(b, 100, 0, 1).await.unwrap();

    assert_ne!(pa.id, pb.id);
  }

  // ---- chunk_size ----

  #[tokio::test]
  async fn chunk_size_returns_size_of_other_part() {
    let db = test_db().await;
    let id = reserve(&db).await;
    let other = reserve(&db).await;
    db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();
    db.cache_upload().chunk(id, 30, 100, 0).await.unwrap();
    db.cache_upload().chunk(other, 7, 0, 1).await.unwrap();

    assert_eq!(
      db.cache_upload().chunk_size(id, 100).await.unwrap(),
      Some(100)
    );
  }

  #[tokio::test]
  async fn chunk_size_ignores_part_at_same_start_and_other_uploads() {
    let db = test_db().await;
    let id = reserve(&db).await;
    let other = reserve(&db).await;
    db.cache_upload().chunk(id, 30, 100, 0).await.unwrap();
    db.cache_upload().chunk(other, 7, 0, 1).await.unwrap();

    assert_eq!(db.cache_upload().chunk_size(id, 100).await.unwrap(), None);
    assert_eq!(db.cache_upload().chunk_size(999, 0).await.unwrap(), None);
  }

  // ---- set_part ----

  #[tokio::test]
  async fn set_part_stores_part_number_and_etag() {
    let db = test_db().await;
    let id = reserve(&db).await;
    let part = db.cache_upload().chunk(id, 30, 100, 0).await.unwrap();

    db.cache_upload()
      .set_part(part.id, 2, "etag".into())
      .await
      .unwrap();

    let parts = db.cache_upload().parts(id).await.unwrap();
    assert_eq!(parts[0].id, part.id);
    assert_eq!(parts[0].part_number, 2);
    assert_eq!(parts[0].start_byte, 100);
    assert_eq!(parts[0].etag.as_deref(), Some("etag"));
  }

  #[tokio::test]
  async fn set_part_missing_part_errors() {
    let db = test_db().await;
    let err = db
      .cache_upload()
      .set_part(999, 1, "etag".into())
      .await
      .unwrap_err();
    assert_eq!(err.error.to_string(), "Cache upload part not found");
  }

  // ---- update_etag ----

  #[tokio::test]
  async fn update_etag_stores_value() {
    let db = test_db().await;
    let id = reserve(&db).await;
    let part = db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();

    db.cache_upload()
      .update_etag(part.id, "etag".into())
      .await
      .unwrap();

    let parts = db.cache_upload().parts(id).await.unwrap();
    assert_eq!(parts[0].etag.as_deref(), Some("etag"));
  }

  #[tokio::test]
  async fn update_etag_missing_part_errors() {
    let db = test_db().await;
    let err = db
      .cache_upload()
      .update_etag(999, "etag".into())
      .await
      .unwrap_err();
    assert_eq!(err.error.to_string(), "Cache upload part not found");
  }

  // ---- find ----

  #[tokio::test]
  async fn find_returns_inserted_upload() {
    let db = test_db().await;
    let row = insert_upload(&db, "repo", "key", "v1", "", now()).await;

    assert_eq!(db.cache_upload().find(row.id).await.unwrap(), row);
  }

  #[tokio::test]
  async fn find_missing_upload_errors() {
    let db = test_db().await;
    let err = db.cache_upload().find(999).await.unwrap_err();
    assert_eq!(err.error.to_string(), "Cache upload entry not found");
  }

  // ---- parts ----

  #[tokio::test]
  async fn parts_maps_all_parts_of_upload() {
    let db = test_db().await;
    let id = reserve(&db).await;
    let other = reserve(&db).await;
    let p1 = db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();
    db.cache_upload().chunk(id, 30, 100, 2).await.unwrap();
    db.cache_upload().chunk(other, 5, 0, 1).await.unwrap();
    db.cache_upload()
      .update_etag(p1.id, "e1".into())
      .await
      .unwrap();

    let mut parts = db.cache_upload().parts(id).await.unwrap();
    parts.sort_by_key(|p| p.part_number);

    assert_eq!(parts.len(), 2);
    assert_eq!(
      (
        parts[0].part_number,
        parts[0].etag.as_deref(),
        parts[0].size
      ),
      (1, Some("e1"), 100)
    );
    assert_eq!(
      (
        parts[1].part_number,
        parts[1].etag.as_deref(),
        parts[1].size
      ),
      (2, None, 30)
    );
  }

  #[tokio::test]
  async fn parts_empty_without_chunks() {
    let db = test_db().await;
    let id = reserve(&db).await;
    assert!(db.cache_upload().parts(id).await.unwrap().is_empty());
  }

  // ---- refresh_created_at ----

  #[tokio::test]
  async fn refresh_created_at_bumps_timestamp() {
    let db = test_db().await;
    let old = now() - Duration::hours(1);
    let row = insert_upload(&db, "repo", "key", "v1", "", old).await;

    db.cache_upload().refresh_created_at(row.id).await.unwrap();

    let row = db.cache_upload().find(row.id).await.unwrap();
    assert!(row.created_at > old + Duration::minutes(59));
  }

  #[tokio::test]
  async fn refresh_created_at_missing_upload_errors() {
    let db = test_db().await;
    let err = db.cache_upload().refresh_created_at(999).await.unwrap_err();
    assert_eq!(err.error.to_string(), "Cache upload entry not found");
  }

  // ---- find_incomplete_uploads ----

  #[tokio::test]
  async fn find_incomplete_uploads_returns_only_older_than_cutoff() {
    let db = test_db().await;
    let cutoff = now();
    let old = insert_upload(&db, "repo", "old", "v1", "", cutoff - Duration::minutes(5)).await;
    insert_upload(&db, "repo", "new", "v1", "", cutoff + Duration::minutes(5)).await;

    let rows = db
      .cache_upload()
      .find_incomplete_uploads(cutoff)
      .await
      .unwrap();

    assert_eq!(rows, vec![old]);
  }

  // ---- delete ----

  #[tokio::test]
  async fn delete_removes_upload_and_its_parts() {
    let db = test_db().await;
    let id = reserve(&db).await;
    db.cache_upload().chunk(id, 100, 0, 1).await.unwrap();

    db.cache_upload().delete(id).await.unwrap();

    assert!(db.cache_upload().find(id).await.is_err());
    assert!(db.cache_upload().parts(id).await.unwrap().is_empty());
  }

  #[tokio::test]
  async fn delete_missing_upload_is_noop() {
    let db = test_db().await;
    db.cache_upload().delete(999).await.unwrap();
  }
}
