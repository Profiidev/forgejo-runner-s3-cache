use centaurus::{
  bail,
  error::{ErrorReport, Result},
  eyre::{Context, ContextCompat},
};
use entity::{cache_cleanup, cache_entry, cache_entry_pending, cache_upload};
use migration::{LikeExpr, OnConflict};
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

          // Remove the old row first: the upsert below would otherwise update it in
          // place and the delete would then drop the freshly completed entry.
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
        .filter(cache_entry::Column::Key.like(LikeExpr::new(wildcard_prefix).escape('\\')))
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

#[cfg(test)]
mod test {
  use centaurus::db::init::Connection;
  use chrono::Duration;
  use entity::{cache_cleanup, cache_entry, cache_entry_pending};
  use sea_orm::{EntityTrait, PaginatorTrait};

  use crate::{
    auth::Auth,
    db::{
      DBTrait,
      test::{insert_entry, insert_pending, insert_upload, now, test_db, test_pg_db},
    },
  };

  fn auth(repo: &str, wik: &str) -> Auth {
    Auth {
      repo: repo.into(),
      write_isolation_key: wik.into(),
    }
  }

  fn keys(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|k| k.to_string()).collect()
  }

  async fn find(db: &Connection, k: &[&str], version: &str, a: Auth) -> Option<String> {
    db.cache_entry()
      .find_entry(&keys(k), version.into(), a)
      .await
      .unwrap()
      .map(|e| e.key)
  }

  /// Runs upload -> pending -> complete for `key` in `repo`/`version`/"".
  async fn upload_and_complete(db: &Connection, key: &str, version: &str) -> (i32, uuid::Uuid) {
    let upload = insert_upload(db, "repo", key, version, "", now()).await;
    let id = upload.id;
    let file_id = upload.file_id;
    db.cache_entry()
      .create_cache_pending(upload, 10)
      .await
      .unwrap();
    db.cache_entry().complete(id).await.unwrap();
    (id, file_id)
  }

  // ---- create_cache_pending ----

  #[tokio::test]
  async fn create_cache_pending_copies_upload_fields() {
    let db = test_db().await;
    let before = now();
    let upload = insert_upload(&db, "repo", "key", "v1", "wik", before).await;

    let id = db
      .cache_entry()
      .create_cache_pending(upload.clone(), 77)
      .await
      .unwrap();

    assert_eq!(id, upload.id);
    let row = cache_entry_pending::Entity::find_by_id(id)
      .one(&db.0)
      .await
      .unwrap()
      .unwrap();
    assert_eq!(row.repo, "repo");
    assert_eq!(row.key, "key");
    assert_eq!(row.version, "v1");
    assert_eq!(row.write_isolation_key, "wik");
    assert_eq!(row.file_id, upload.file_id);
    assert_eq!(row.size, 77);
    assert_eq!(row.created_at, row.used_at);
    assert!(row.created_at >= before);
  }

  // ---- find_by_id ----

  #[tokio::test]
  async fn find_by_id_returns_entry() {
    let db = test_db().await;
    let entry = insert_entry(&db, "repo", "key", "v1", "", now(), now()).await;
    assert_eq!(
      db.cache_entry().find_by_id(entry.id).await.unwrap(),
      Some(entry)
    );
  }

  #[tokio::test]
  async fn find_by_id_missing_returns_none() {
    let db = test_db().await;
    assert_eq!(db.cache_entry().find_by_id(999).await.unwrap(), None);
  }

  // ---- complete ----

  #[tokio::test]
  async fn complete_moves_pending_into_cache_entry() {
    let db = test_db().await;
    let (id, file_id) = upload_and_complete(&db, "key", "v1").await;

    let entry = db.cache_entry().find_by_id(id).await.unwrap().unwrap();
    assert_eq!(entry.key, "key");
    assert_eq!(entry.repo, "repo");
    assert_eq!(entry.version, "v1");
    assert_eq!(entry.size, 10);
    assert_eq!(entry.file_id, file_id);
    assert!(
      cache_entry_pending::Entity::find_by_id(id)
        .one(&db.0)
        .await
        .unwrap()
        .is_none()
    );
    assert!(db.cache_upload().find(id).await.is_err());
    assert_eq!(cache_cleanup::Entity::find().count(&db.0).await.unwrap(), 0);
  }

  #[tokio::test]
  async fn complete_missing_pending_errors() {
    let db = test_db().await;
    let err = db.cache_entry().complete(999).await.unwrap_err();
    assert_eq!(err.error.to_string(), "Failed to complete upload");
    assert!(format!("{:?}", err.error).contains("Cache pending entry not found"));
  }

  async fn assert_complete_replaces_existing(db: &Connection) {
    let (old_id, old_file) = upload_and_complete(db, "key", "v1").await;
    let (new_id, new_file) = upload_and_complete(db, "key", "v1").await;

    let entries = cache_entry::Entity::find().all(&db.0).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, new_id);
    assert_eq!(entries[0].file_id, new_file);
    assert!(db.cache_entry().find_by_id(old_id).await.unwrap().is_none());

    let cleanup = cache_cleanup::Entity::find().all(&db.0).await.unwrap();
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0].file_id, old_file);
  }

  #[tokio::test]
  async fn complete_replaces_existing_entry_and_schedules_old_file_cleanup() {
    let db = test_db().await;
    assert_complete_replaces_existing(&db).await;
  }

  #[tokio::test]
  async fn complete_different_version_keeps_both_entries() {
    let db = test_db().await;
    upload_and_complete(&db, "key", "v0").await;
    upload_and_complete(&db, "key", "v1").await;

    assert_eq!(cache_entry::Entity::find().count(&db.0).await.unwrap(), 2);
    assert_eq!(cache_cleanup::Entity::find().count(&db.0).await.unwrap(), 0);
  }

  // ---- update_used_at ----

  #[tokio::test]
  async fn update_used_at_bumps_timestamp() {
    let db = test_db().await;
    let old = now() - Duration::hours(1);
    let entry = insert_entry(&db, "repo", "key", "v1", "", old, old).await;

    db.cache_entry().update_used_at(entry.id).await.unwrap();

    let row = db
      .cache_entry()
      .find_by_id(entry.id)
      .await
      .unwrap()
      .unwrap();
    assert!(row.used_at > old + Duration::minutes(59));
    assert_eq!(row.created_at, old);
  }

  #[tokio::test]
  async fn update_used_at_missing_entry_errors() {
    let db = test_db().await;
    let err = db.cache_entry().update_used_at(999).await.unwrap_err();
    assert_eq!(err.error.to_string(), "Cache entry not found");
  }

  // ---- find_entry ----

  #[tokio::test]
  async fn find_entry_exact_key_beats_newer_prefix_match() {
    let db = test_db().await;
    let t = now();
    insert_entry(&db, "repo", "key", "v1", "", t - Duration::hours(1), t).await;
    insert_entry(&db, "repo", "key-newer", "v1", "", t, t).await;

    assert_eq!(
      find(&db, &["key"], "v1", auth("repo", "")).await.as_deref(),
      Some("key")
    );
  }

  #[tokio::test]
  async fn find_entry_prefix_match_returns_newest() {
    let db = test_db().await;
    let t = now();
    insert_entry(&db, "repo", "key-a", "v1", "", t - Duration::hours(1), t).await;
    insert_entry(&db, "repo", "key-b", "v1", "", t, t).await;
    insert_entry(&db, "repo", "key-c", "v1", "", t - Duration::hours(2), t).await;

    assert_eq!(
      find(&db, &["key-"], "v1", auth("repo", ""))
        .await
        .as_deref(),
      Some("key-b")
    );
  }

  #[tokio::test]
  async fn find_entry_earlier_key_in_list_has_priority() {
    let db = test_db().await;
    let t = now();
    insert_entry(&db, "repo", "first-x", "v1", "", t - Duration::hours(1), t).await;
    insert_entry(&db, "repo", "second", "v1", "", t, t).await;

    assert_eq!(
      find(&db, &["first", "second"], "v1", auth("repo", ""))
        .await
        .as_deref(),
      Some("first-x")
    );
    assert_eq!(
      find(&db, &["missing", "second"], "v1", auth("repo", ""))
        .await
        .as_deref(),
      Some("second")
    );
  }

  #[tokio::test]
  async fn find_entry_version_or_repo_mismatch_returns_none() {
    let db = test_db().await;
    insert_entry(&db, "repo", "key", "v1", "", now(), now()).await;

    assert_eq!(find(&db, &["key"], "v2", auth("repo", "")).await, None);
    assert_eq!(find(&db, &["key"], "v1", auth("other", "")).await, None);
  }

  #[tokio::test]
  async fn find_entry_prefers_wik_scoped_entry() {
    let db = test_db().await;
    let t = now();
    insert_entry(&db, "repo", "key", "v1", "", t, t).await;
    let scoped = insert_entry(&db, "repo", "key", "v1", "wik", t - Duration::hours(1), t).await;

    let found = db
      .cache_entry()
      .find_entry(&keys(&["key"]), "v1".into(), auth("repo", "wik"))
      .await
      .unwrap();
    assert_eq!(found, Some(scoped));
  }

  #[tokio::test]
  async fn find_entry_falls_back_to_unscoped_entry() {
    let db = test_db().await;
    let unscoped = insert_entry(&db, "repo", "key", "v1", "", now(), now()).await;
    insert_entry(&db, "repo", "key", "v1", "other-wik", now(), now()).await;

    let found = db
      .cache_entry()
      .find_entry(&keys(&["key"]), "v1".into(), auth("repo", "wik"))
      .await
      .unwrap();
    assert_eq!(found, Some(unscoped));
  }

  #[tokio::test]
  async fn find_entry_without_wik_ignores_scoped_entries() {
    let db = test_db().await;
    insert_entry(&db, "repo", "key", "v1", "wik", now(), now()).await;

    assert_eq!(find(&db, &["key"], "v1", auth("repo", "")).await, None);
  }

  #[tokio::test]
  async fn find_entry_empty_keys_returns_none() {
    let db = test_db().await;
    insert_entry(&db, "repo", "key", "v1", "", now(), now()).await;

    assert_eq!(find(&db, &[], "v1", auth("repo", "")).await, None);
  }

  async fn assert_like_wildcards_match_literally(db: &Connection) {
    for key in ["a%b-1", "a_b-1", "a\\b-1"] {
      insert_entry(db, "repo", key, "v1", "", now(), now()).await;
    }
    insert_entry(
      db,
      "repo",
      "aXb-decoy",
      "v1",
      "",
      now() + Duration::hours(1),
      now(),
    )
    .await;

    for prefix in ["a%b", "a_b", "a\\b"] {
      assert_eq!(
        find(db, &[prefix], "v1", auth("repo", "")).await,
        Some(format!("{prefix}-1")),
        "prefix {prefix:?}"
      );
    }
    assert_eq!(
      find(db, &["a%"], "v1", auth("repo", "")).await.as_deref(),
      Some("a%b-1")
    );
    assert_eq!(find(db, &["%"], "v1", auth("repo", "")).await, None);
    assert_eq!(find(db, &["_"], "v1", auth("repo", "")).await, None);
  }

  #[tokio::test]
  async fn find_entry_prefix_treats_like_wildcards_literally() {
    let db = test_db().await;
    assert_like_wildcards_match_literally(&db).await;
  }

  // ---- cutoff queries ----

  #[tokio::test]
  async fn find_incomplete_entries_returns_only_pending_older_than_cutoff() {
    let db = test_db().await;
    let cutoff = now();
    let old = insert_pending(&db, 1, "old", cutoff - Duration::minutes(5)).await;
    insert_pending(&db, 2, "new", cutoff + Duration::minutes(5)).await;

    let rows = db
      .cache_entry()
      .find_incomplete_entries(cutoff)
      .await
      .unwrap();
    assert_eq!(rows, vec![old]);
  }

  #[tokio::test]
  async fn find_unused_entries_filters_on_used_at() {
    let db = test_db().await;
    let cutoff = now();
    let old = cutoff - Duration::minutes(5);
    let new = cutoff + Duration::minutes(5);
    let unused = insert_entry(&db, "repo", "unused", "v1", "", new, old).await;
    insert_entry(&db, "repo", "used", "v1", "", old, new).await;

    let rows = db.cache_entry().find_unused_entries(cutoff).await.unwrap();
    assert_eq!(rows, vec![unused]);
  }

  #[tokio::test]
  async fn find_entries_before_filters_on_created_at() {
    let db = test_db().await;
    let cutoff = now();
    let old = cutoff - Duration::minutes(5);
    let new = cutoff + Duration::minutes(5);
    let created_old = insert_entry(&db, "repo", "old", "v1", "", old, new).await;
    insert_entry(&db, "repo", "new", "v1", "", new, old).await;

    let rows = db.cache_entry().find_entries_before(cutoff).await.unwrap();
    assert_eq!(rows, vec![created_old]);
  }

  // ---- delete ----

  #[tokio::test]
  async fn delete_by_id_removes_entry_and_is_idempotent() {
    let db = test_db().await;
    let entry = insert_entry(&db, "repo", "key", "v1", "", now(), now()).await;

    db.cache_entry().delete_by_id(entry.id).await.unwrap();
    db.cache_entry().delete_by_id(entry.id).await.unwrap();

    assert_eq!(db.cache_entry().find_by_id(entry.id).await.unwrap(), None);
  }

  #[tokio::test]
  async fn delete_pending_by_id_removes_pending_and_is_idempotent() {
    let db = test_db().await;
    insert_pending(&db, 1, "key", now()).await;

    db.cache_entry().delete_pending_by_id(1).await.unwrap();
    db.cache_entry().delete_pending_by_id(1).await.unwrap();

    assert_eq!(
      cache_entry_pending::Entity::find()
        .count(&db.0)
        .await
        .unwrap(),
      0
    );
  }

  // ---- postgres ----

  #[tokio::test]
  async fn pg_complete_replaces_existing_entry_and_schedules_old_file_cleanup() {
    let (db, _container) = test_pg_db().await;
    assert_complete_replaces_existing(&db).await;
  }

  #[tokio::test]
  async fn pg_find_entry_prefix_treats_like_wildcards_literally() {
    let (db, _container) = test_pg_db().await;
    assert_like_wildcards_match_literally(&db).await;
  }

  #[tokio::test]
  async fn pg_find_entry_exact_prefix_and_wik_fallback() {
    let (db, _container) = test_pg_db().await;
    let t = now();
    insert_entry(&db, "repo", "key", "v1", "", t - Duration::hours(1), t).await;
    insert_entry(&db, "repo", "key-newer", "v1", "", t, t).await;

    assert_eq!(
      find(&db, &["key"], "v1", auth("repo", "wik"))
        .await
        .as_deref(),
      Some("key")
    );
    assert_eq!(
      find(&db, &["key-"], "v1", auth("repo", ""))
        .await
        .as_deref(),
      Some("key-newer")
    );
    assert_eq!(find(&db, &["key"], "v2", auth("repo", "")).await, None);
  }
}
