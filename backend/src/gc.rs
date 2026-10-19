use std::sync::Arc;

use axum::{Extension, Router};
use centaurus::{
  db::init::Connection,
  storage::{FileStorage, StoragePath},
};
use chrono::{Duration, Utc};
use entity::{cache_cleanup, cache_entry};
use sea_orm::EntityTrait;
use tokio::{spawn, task::JoinHandle, time::sleep};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
  db::DBTrait,
  storage::{StorageExt, tail_path},
};

pub fn state(router: Router, db: Connection, storage: FileStorage) -> Router {
  router.layer(Extension(Gc::init(db, storage)))
}

#[derive(Clone)]
struct Gc {
  _entry: Arc<JoinHandle<()>>,
  _unused_entry: Arc<JoinHandle<()>>,
  _incomplete_entry: Arc<JoinHandle<()>>,
  _cache_cleanup: Arc<JoinHandle<()>>,
  _upload: Arc<JoinHandle<()>>,
}

impl Gc {
  pub fn init(db: Connection, storage: FileStorage) -> Self {
    let entry = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        entry_gc(db, storage).await;
      }
    });

    let unused_entry = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        unused_entry_gc(db, storage).await;
      }
    });

    let incomplete_entry = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        incomplete_entry_gc(db, storage).await;
      }
    });

    let cache_cleanup = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        cache_cleanup_gc(db, storage).await;
      }
    });

    let upload = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        upload_gc(db, storage).await;
      }
    });

    Gc {
      _entry: Arc::new(entry),
      _unused_entry: Arc::new(unused_entry),
      _incomplete_entry: Arc::new(incomplete_entry),
      _cache_cleanup: Arc::new(cache_cleanup),
      _upload: Arc::new(upload),
    }
  }
}

async fn entry_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    entry_gc_pass(&db, &storage).await;
    sleep(Duration::hours(24).to_std().unwrap()).await;
  }
}

async fn entry_gc_pass(db: &Connection, storage: &FileStorage) {
  info!("Running GC for cache entries");
  // cleanup entries that are older than 30 days
  let before = Utc::now() - Duration::days(30);
  let Ok(entries) = db
    .cache_entry()
    .find_entries_before(before.naive_utc())
    .await
    .map_err(|e| {
      warn!("Failed to query cache entries for GC: {e}");
    })
  else {
    return;
  };

  info!("Found {} cache entries for GC", entries.len());

  delete_entries(db, storage, entries).await;
}

async fn unused_entry_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    unused_entry_gc_pass(&db, &storage).await;
    sleep(Duration::hours(1).to_std().unwrap()).await;
  }
}

async fn unused_entry_gc_pass(db: &Connection, storage: &FileStorage) {
  info!("Running GC for unused cache entries");
  // cleanup entries that haven't been used for more than 7 days
  let before = Utc::now() - Duration::days(7);
  let Ok(entries) = db
    .cache_entry()
    .find_unused_entries(before.naive_utc())
    .await
    .map_err(|e| {
      warn!("Failed to query unused cache entries for GC: {e}");
    })
  else {
    return;
  };

  info!("Found {} unused cache entries for GC", entries.len());

  delete_entries(db, storage, entries).await;
}

async fn delete_entries(db: &Connection, storage: &FileStorage, entries: Vec<cache_entry::Model>) {
  for entry in entries {
    if !try_delete_file(storage, &file_path(entry.file_id)).await {
      continue;
    }

    if let Err(e) = db.cache_entry().delete_by_id(entry.id).await {
      warn!("Failed to delete cache entry for GC: {e}");
    }
  }
}

async fn incomplete_entry_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    incomplete_entry_gc_pass(&db, &storage).await;
    sleep(Duration::minutes(10).to_std().unwrap()).await;
  }
}

async fn incomplete_entry_gc_pass(db: &Connection, storage: &FileStorage) {
  info!("Running GC for incomplete cache entries");
  // cleanup incomplete entries that failed to be marked complete for more than 10 minutes
  let before = Utc::now() - Duration::minutes(10);
  let Ok(entries) = db
    .cache_entry()
    .find_incomplete_entries(before.naive_utc())
    .await
    .map_err(|e| {
      warn!("Failed to query incomplete cache entries for GC: {e}");
    })
  else {
    return;
  };

  info!("Found {} incomplete cache entries for GC", entries.len());

  for entry in entries {
    if !try_delete_file(storage, &file_path(entry.file_id)).await {
      continue;
    }

    if let Err(e) = db.cache_entry().delete_pending_by_id(entry.id).await {
      warn!("Failed to delete incomplete cache entry for GC: {e}");
    }
  }
}

async fn cache_cleanup_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    cache_cleanup_gc_pass(&db, &storage).await;
    sleep(Duration::minutes(5).to_std().unwrap()).await;
  }
}

async fn cache_cleanup_gc_pass(db: &Connection, storage: &FileStorage) {
  info!("Running GC for cache cleanup entries");
  // cleanup cache cleanup entries that where marked for cleanup because of commit conflicts
  let Ok(entries) = cache_cleanup::Entity::find().all(&db.0).await.map_err(|e| {
    warn!("Failed to query cache cleanup entries for GC: {e}");
  }) else {
    return;
  };

  info!("Found {} cache cleanup entries for GC", entries.len());

  for entry in entries {
    if !try_delete_file(storage, &file_path(entry.file_id)).await {
      continue;
    }

    if let Err(e) = cache_cleanup::Entity::delete_by_id(entry.id)
      .exec(&db.0)
      .await
    {
      warn!("Failed to delete cache cleanup entry for GC: {e}");
    }
  }
}

fn file_path(file_id: Uuid) -> StoragePath {
  StoragePath::from(file_id.to_string())
}

async fn try_delete_file(storage: &FileStorage, path: &StoragePath) -> bool {
  let Ok(exists) = storage.exists(path).await.map_err(|e| {
    warn!("Failed to check cache object existence for GC: {e}");
  }) else {
    return false;
  };

  if exists && let Err(e) = storage.delete_file(path).await {
    warn!("Failed to delete cache object for GC: {e}");
    return false;
  }

  true
}

async fn upload_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    upload_gc_pass(&db, &storage).await;
    sleep(Duration::minutes(10).to_std().unwrap()).await;
  }
}

async fn upload_gc_pass(db: &Connection, storage: &FileStorage) {
  info!("Running GC for incomplete uploads");
  // cleanup uploads that didn't receive a new part for more than 10 minutes
  let before = Utc::now() - Duration::minutes(10);
  let Ok(uploads) = db
    .cache_upload()
    .find_incomplete_uploads(before.naive_utc())
    .await
    .map_err(|e| {
      warn!("Failed to query incomplete uploads for GC: {e}");
    })
  else {
    return;
  };

  info!("Found {} incomplete uploads for GC", uploads.len());

  for upload in uploads {
    // Before the abort, so a failed delete is retried on the next pass.
    if !try_delete_file(storage, &tail_path(upload.file_id)).await {
      continue;
    }

    // Only unset if reserve died between creating the row and the S3 upload,
    // in which case there is nothing to abort.
    if let Some(upload_id) = &upload.s3_upload_id
      && storage
        .cancel_multipart_upload(&file_path(upload.file_id), upload_id)
        .await
        .map_err(|e| {
          warn!("Failed to cancel multipart upload for GC: {e}");
        })
        .is_err()
    {
      continue;
    }

    if let Err(e) = db.cache_upload().delete(upload.id).await {
      warn!("Failed to delete cache upload entry for GC: {e}");
    }
  }
}

#[cfg(test)]
mod test {
  use axum::{Router, body::Bytes};
  use centaurus::{
    db::init::Connection,
    storage::{FileStorage, StoragePath},
  };
  use chrono::{Duration, NaiveDateTime};
  use entity::{cache_cleanup, cache_entry, cache_entry_pending, cache_upload};
  use sea_orm::{ActiveModelTrait, ActiveValue::NotSet, EntityTrait, IntoActiveModel, Set};
  use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};
  use uuid::Uuid;

  use super::{
    Gc, cache_cleanup_gc_pass, entry_gc_pass, incomplete_entry_gc_pass, state, try_delete_file,
    unused_entry_gc_pass, upload_gc_pass,
  };
  use crate::{
    db::test::{insert_entry, insert_upload, now, test_db},
    storage::{
      StorageExt, tail_path,
      test::{init_fs_storage, init_s3_storage},
    },
  };

  const REPO: &str = "owner/repo";

  struct Ctx {
    db: Connection,
    storage: FileStorage,
    _container: ContainerAsync<GenericImage>,
  }

  async fn ctx() -> Ctx {
    let (storage, container) = init_s3_storage().await;
    Ctx {
      db: test_db().await,
      storage,
      _container: container,
    }
  }

  fn path(file_id: Uuid) -> StoragePath {
    StoragePath::from(file_id.to_string())
  }

  async fn put_file(storage: &FileStorage, file_id: Uuid) {
    storage
      .save_file(&mut &b"data"[..], path(file_id))
      .await
      .unwrap();
  }

  async fn exists(storage: &FileStorage, file_id: Uuid) -> bool {
    storage.exists(&path(file_id)).await.unwrap()
  }

  async fn entry(ctx: &Ctx, key: &str, created_at: NaiveDateTime, used_at: NaiveDateTime) -> Uuid {
    let entry = insert_entry(&ctx.db, REPO, key, "v1", "", created_at, used_at).await;
    put_file(&ctx.storage, entry.file_id).await;
    entry.file_id
  }

  async fn entry_ids(db: &Connection) -> Vec<Uuid> {
    cache_entry::Entity::find()
      .all(&db.0)
      .await
      .unwrap()
      .into_iter()
      .map(|e| e.file_id)
      .collect()
  }

  async fn pending(ctx: &Ctx, id: i32, created_at: NaiveDateTime) -> Uuid {
    let file_id = Uuid::new_v4();
    cache_entry_pending::ActiveModel {
      id: Set(id),
      repo: Set(REPO.into()),
      key: Set(format!("key-{id}")),
      version: Set("v1".into()),
      write_isolation_key: Set(String::new()),
      size: Set(4),
      created_at: Set(created_at),
      used_at: Set(created_at),
      file_id: Set(file_id),
    }
    .insert(&ctx.db.0)
    .await
    .unwrap();
    put_file(&ctx.storage, file_id).await;
    file_id
  }

  // ---- try_delete_file ----

  #[tokio::test]
  async fn try_delete_file_on_missing_object_succeeds() {
    let (storage, _container) = init_s3_storage().await;
    assert!(try_delete_file(&storage, &path(Uuid::new_v4())).await);
  }

  #[tokio::test]
  async fn try_delete_file_deletes_existing_object() {
    let (storage, _container) = init_s3_storage().await;
    let file_id = Uuid::new_v4();
    put_file(&storage, file_id).await;

    assert!(try_delete_file(&storage, &path(file_id)).await);
    assert!(!exists(&storage, file_id).await);
  }

  // ---- entry_gc_pass ----

  #[tokio::test]
  async fn entry_gc_pass_deletes_entries_older_than_30_days() {
    let ctx = ctx().await;
    let stale = entry(&ctx, "stale", now() - Duration::days(31), now()).await;
    let fresh = entry(&ctx, "fresh", now() - Duration::days(29), now()).await;

    entry_gc_pass(&ctx.db, &ctx.storage).await;

    assert_eq!(entry_ids(&ctx.db).await, vec![fresh]);
    assert!(!exists(&ctx.storage, stale).await);
    assert!(exists(&ctx.storage, fresh).await);
  }

  // ---- unused_entry_gc_pass ----

  #[tokio::test]
  async fn unused_entry_gc_pass_deletes_entries_unused_for_7_days() {
    let ctx = ctx().await;
    let stale = entry(&ctx, "stale", now(), now() - Duration::days(8)).await;
    let fresh = entry(&ctx, "fresh", now(), now() - Duration::days(6)).await;

    unused_entry_gc_pass(&ctx.db, &ctx.storage).await;

    assert_eq!(entry_ids(&ctx.db).await, vec![fresh]);
    assert!(!exists(&ctx.storage, stale).await);
    assert!(exists(&ctx.storage, fresh).await);
  }

  #[tokio::test]
  async fn unused_entry_gc_pass_deletes_row_whose_object_is_already_gone() {
    let ctx = ctx().await;
    let old = now() - Duration::days(8);
    insert_entry(&ctx.db, REPO, "key", "v1", "", old, old).await;

    unused_entry_gc_pass(&ctx.db, &ctx.storage).await;

    assert!(entry_ids(&ctx.db).await.is_empty());
  }

  // ---- incomplete_entry_gc_pass ----

  #[tokio::test]
  async fn incomplete_entry_gc_pass_deletes_pending_entries_older_than_10_minutes() {
    let ctx = ctx().await;
    let stale = pending(&ctx, 1, now() - Duration::minutes(11)).await;
    let fresh = pending(&ctx, 2, now() - Duration::minutes(9)).await;

    incomplete_entry_gc_pass(&ctx.db, &ctx.storage).await;

    let rows = cache_entry_pending::Entity::find()
      .all(&ctx.db.0)
      .await
      .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].file_id, fresh);
    assert!(!exists(&ctx.storage, stale).await);
    assert!(exists(&ctx.storage, fresh).await);
  }

  // ---- cache_cleanup_gc_pass ----

  #[tokio::test]
  async fn cache_cleanup_gc_pass_deletes_all_rows_and_objects() {
    let ctx = ctx().await;
    let with_object = Uuid::new_v4();
    let without_object = Uuid::new_v4();
    put_file(&ctx.storage, with_object).await;
    for file_id in [with_object, without_object] {
      cache_cleanup::ActiveModel {
        id: NotSet,
        file_id: Set(file_id),
      }
      .insert(&ctx.db.0)
      .await
      .unwrap();
    }

    cache_cleanup_gc_pass(&ctx.db, &ctx.storage).await;

    assert!(
      cache_cleanup::Entity::find()
        .all(&ctx.db.0)
        .await
        .unwrap()
        .is_empty()
    );
    assert!(!exists(&ctx.storage, with_object).await);
  }

  // ---- upload_gc_pass ----

  #[tokio::test]
  async fn upload_gc_pass_aborts_stale_multipart_uploads_and_deletes_rows() {
    let ctx = ctx().await;
    let old = now() - Duration::minutes(11);

    let stale = insert_upload(&ctx.db, REPO, "stale", "v1", "", old).await;
    let key = path(stale.file_id);
    let upload_id = ctx.storage.create_multipart_upload(&key).await.unwrap();
    let mut model = stale.clone().into_active_model();
    model.s3_upload_id = Set(Some(upload_id.clone()));
    model.update(&ctx.db.0).await.unwrap();

    // Row without s3_upload_id: nothing to abort, row still goes.
    insert_upload(&ctx.db, REPO, "orphan", "v1", "", old).await;
    let fresh = insert_upload(&ctx.db, REPO, "fresh", "v1", "", now()).await;

    upload_gc_pass(&ctx.db, &ctx.storage).await;

    let rows = cache_upload::Entity::find().all(&ctx.db.0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, fresh.id);

    // The multipart upload no longer accepts parts.
    assert!(
      ctx
        .storage
        .upload_part(&key, &upload_id, 1, Bytes::from_static(b"data"))
        .await
        .is_err()
    );
  }

  #[tokio::test]
  async fn upload_gc_pass_deletes_parked_tail_object() {
    let ctx = ctx().await;
    let stale = insert_upload(
      &ctx.db,
      REPO,
      "stale",
      "v1",
      "",
      now() - Duration::minutes(11),
    )
    .await;
    let tail = tail_path(stale.file_id);
    ctx
      .storage
      .save_file(&mut &b"tail"[..], tail.clone())
      .await
      .unwrap();

    upload_gc_pass(&ctx.db, &ctx.storage).await;

    assert!(!ctx.storage.exists(&tail).await.unwrap());
    assert!(
      cache_upload::Entity::find()
        .all(&ctx.db.0)
        .await
        .unwrap()
        .is_empty()
    );
  }

  #[tokio::test]
  async fn upload_gc_pass_keeps_row_when_abort_fails() {
    let db = test_db().await;
    // Local storage has no multipart API, so cancelling always fails.
    let storage = init_fs_storage().await;
    let stale = insert_upload(&db, REPO, "stale", "v1", "", now() - Duration::minutes(11)).await;
    let mut model = stale.clone().into_active_model();
    model.s3_upload_id = Set(Some("upload".into()));
    model.update(&db.0).await.unwrap();

    upload_gc_pass(&db, &storage).await;

    assert!(
      cache_upload::Entity::find_by_id(stale.id)
        .one(&db.0)
        .await
        .unwrap()
        .is_some()
    );
  }

  // ---- init ----

  #[tokio::test]
  async fn gc_init_and_state_spawn_without_panicking() {
    let db = test_db().await;
    let storage = init_fs_storage().await;

    let gc = Gc::init(db.clone(), storage.clone());
    let _router: Router = state(Router::new(), db, storage);

    // Let the first passes run on the empty database.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for handle in [
      &gc._entry,
      &gc._unused_entry,
      &gc._incomplete_entry,
      &gc._cache_cleanup,
      &gc._upload,
    ] {
      assert!(!handle.is_finished());
    }
  }
}
