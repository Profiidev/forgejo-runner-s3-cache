use axum::{
  Json, Router,
  body::Bytes,
  extract::{DefaultBodyLimit, Path},
  routing::{patch, post},
};
use axum_extra::{TypedHeader, headers::ContentRange};
use centaurus::{
  bail,
  db::init::Connection,
  error::{ErrorReportStatusExt, Result},
  storage::{FileStorage, StoragePath},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
  auth::Auth,
  db::DBTrait,
  storage::{StorageExt, tail_path},
};

pub fn router() -> Router {
  Router::new()
    .route("/caches", post(reserve))
    .route(
      "/caches/{id}",
      patch(upload_chunk).layer(DefaultBodyLimit::max(50 * 1024 * 1024)),
    )
    .route("/caches/{id}", post(commit))
    .route("/clean", post(clean))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReserveRequest {
  key: String,
  version: String,
  cache_size: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReserveResponse {
  cache_id: i32,
}

async fn reserve(
  auth: Auth,
  db: Connection,
  storage: FileStorage,
  Json(mut req): Json<ReserveRequest>,
) -> Result<Json<ReserveResponse>> {
  if let Some(size) = req.cache_size {
    if size == -1 {
      req.cache_size = None;
    } else if size < 0 {
      bail!("Invalid cache size");
    }
  }

  let (cache_id, file_id) = db
    .cache_upload()
    .reserve(
      req.key.to_lowercase(),
      req.version,
      req.cache_size,
      auth.repo,
      auth.write_isolation_key,
    )
    .await?;

  let upload_id = storage
    .create_multipart_upload(&StoragePath::from(file_id.to_string()))
    .await?;

  db.cache_upload()
    .set_s3_upload_id(cache_id, upload_id)
    .await?;

  Ok(Json(ReserveResponse { cache_id }))
}

#[derive(Deserialize)]
struct UploadChunkPath {
  id: i32,
}

/// Only unset if `reserve` died between creating the row and the S3 upload.
fn upload_id(upload: &entity::cache_upload::Model) -> Result<String> {
  upload.s3_upload_id.clone().status_context(
    StatusCode::INTERNAL_SERVER_ERROR,
    "Cache upload has no S3 upload ID",
  )
}

async fn upload_chunk(
  auth: Auth,
  storage: FileStorage,
  db: Connection,
  TypedHeader(range): TypedHeader<ContentRange>,
  Path(path): Path<UploadChunkPath>,
  req: Bytes,
) -> Result<()> {
  let Some((start, _)) = range.bytes_range() else {
    bail!("Content-Range header must specify a byte range");
  };

  // Also guards the division below.
  if req.is_empty() {
    bail!("Chunk must not be empty");
  }

  let upload = db.cache_upload().find(path.id).await?;

  if auth.repo != upload.repo || auth.write_isolation_key != upload.write_isolation_key {
    bail!(FORBIDDEN, "Write isolation key does not match");
  }

  let start = start as i64;
  let len = req.len() as i64;

  // Clients send fixed-size chunks plus a smaller tail, so the tail's part
  // number has to come from the size of another chunk.
  let chunk_size = if start > 0 && upload.size == Some(start + len) {
    db.cache_upload().chunk_size(path.id, start).await?
  } else {
    Some(len)
  };

  let Some(chunk_size) = chunk_size else {
    // The tail arrived first; park it until commit knows the chunk size.
    db.cache_upload().chunk(path.id, len, start, 0).await?;
    storage
      .save_file(&mut &req[..], tail_path(upload.file_id))
      .await?;
    db.cache_upload().refresh_created_at(path.id).await?;
    return Ok(());
  };

  let part_number = start / chunk_size + 1;

  let chunk = db
    .cache_upload()
    .chunk(path.id, len, start, part_number as i32)
    .await?;

  let etag = storage
    .upload_part(
      &StoragePath::from(upload.file_id.to_string()),
      &upload_id(&upload)?,
      chunk.part_number,
      req,
    )
    .await?;

  db.cache_upload().update_etag(chunk.id, etag).await?;

  db.cache_upload().refresh_created_at(path.id).await?;

  Ok(())
}

async fn commit(
  auth: Auth,
  storage: FileStorage,
  db: Connection,
  Path(path): Path<UploadChunkPath>,
) -> Result<()> {
  let upload = db.cache_upload().find(path.id).await?;

  if auth.repo != upload.repo || auth.write_isolation_key != upload.write_isolation_key {
    bail!(FORBIDDEN, "Write isolation key does not match");
  }

  let mut parts = db.cache_upload().parts(upload.id).await?;

  let size: i64 = parts.iter().map(|part| part.size).sum();

  if let Some(expected_size) = upload.size
    && expected_size >= 0
    && size != expected_size
  {
    bail!(
      "Broken file: final size {} does not match reserved size {}",
      size,
      expected_size
    );
  }

  let key = StoragePath::from(upload.file_id.to_string());
  let upload_id = upload_id(&upload)?;

  // A tail parked by `upload_chunk` (part number 0) gets its part number now.
  let chunk_size = parts
    .iter()
    .find(|part| part.part_number != 0)
    .map(|part| part.size);
  for part in parts.iter_mut().filter(|part| part.part_number == 0) {
    let Some(chunk_size) = chunk_size else {
      bail!("Cannot determine chunk size");
    };
    part.part_number = (part.start_byte / chunk_size + 1) as i32;

    let body = storage.get_file(&tail_path(upload.file_id), None).await?;
    let data = axum::body::to_bytes(body, usize::MAX)
      .await
      .status(StatusCode::INTERNAL_SERVER_ERROR)?;
    let etag = storage
      .upload_part(&key, &upload_id, part.part_number, data)
      .await?;
    db.cache_upload()
      .set_part(part.id, part.part_number, etag.clone())
      .await?;
    part.etag = Some(etag);
  }

  let cache = db
    .cache_entry()
    .create_cache_pending(upload.clone(), size)
    .await?;

  storage
    .complete_multipart_upload(&key, &upload_id, parts)
    .await?;

  // Also covers a parked tail that was later retried once the size was known.
  if let Err(e) = storage.delete_file(&tail_path(upload.file_id)).await {
    warn!("Failed to delete parked tail chunk: {e}");
  }

  db.cache_entry().complete(cache).await?;

  Ok(())
}

async fn clean(_auth: Auth) -> Result<()> {
  Ok(())
}

#[cfg(test)]
mod test {
  use axum::{Json, body::Bytes, extract::Path};
  use axum_extra::{TypedHeader, headers::ContentRange};
  use centaurus::{
    db::init::Connection,
    error::Result,
    storage::{FileStorage, StoragePath},
  };
  use chrono::Duration;
  use entity::{cache_cleanup, cache_entry, cache_entry_pending, cache_upload, cache_upload_part};
  use http::StatusCode;
  use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, Set};
  use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

  use super::{ReserveRequest, UploadChunkPath, clean, commit, reserve, upload_chunk, upload_id};
  use crate::{
    auth::Auth,
    db::test::{insert_upload, now, test_db},
    storage::{
      tail_path,
      test::{MIN_PART, big_chunk, init_s3_storage},
    },
  };

  const REPO: &str = "owner/repo";
  const WIK: &str = "refs/heads/main";

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

  fn auth(wik: &str) -> Auth {
    Auth {
      repo: REPO.into(),
      write_isolation_key: wik.into(),
    }
  }

  async fn do_reserve(ctx: &Ctx, key: &str, cache_size: Option<i64>) -> Result<i32> {
    let Json(res) = reserve(
      auth(WIK),
      ctx.db.clone(),
      ctx.storage.clone(),
      Json(ReserveRequest {
        key: key.into(),
        version: "v1".into(),
        cache_size,
      }),
    )
    .await?;
    Ok(res.cache_id)
  }

  async fn do_upload(ctx: &Ctx, wik: &str, id: i32, start: u64, data: Bytes) -> Result<()> {
    let end = start + data.len() as u64;
    upload_chunk(
      auth(wik),
      ctx.storage.clone(),
      ctx.db.clone(),
      TypedHeader(ContentRange::bytes(start..end, None).unwrap()),
      Path(UploadChunkPath { id }),
      data,
    )
    .await
  }

  async fn do_commit(ctx: &Ctx, wik: &str, id: i32) -> Result<()> {
    commit(
      auth(wik),
      ctx.storage.clone(),
      ctx.db.clone(),
      Path(UploadChunkPath { id }),
    )
    .await
  }

  async fn find_upload(db: &Connection, id: i32) -> Option<cache_upload::Model> {
    cache_upload::Entity::find_by_id(id)
      .one(&db.0)
      .await
      .unwrap()
  }

  async fn parts_of(db: &Connection, id: i32) -> Vec<cache_upload_part::Model> {
    cache_upload_part::Entity::find()
      .filter(cache_upload_part::Column::CacheUpload.eq(id))
      .all(&db.0)
      .await
      .unwrap()
  }

  async fn read(storage: &FileStorage, file_id: uuid::Uuid) -> Vec<u8> {
    let body = storage
      .get_file(&StoragePath::from(file_id.to_string()), None)
      .await
      .unwrap();
    axum::body::to_bytes(body, usize::MAX)
      .await
      .unwrap()
      .to_vec()
  }

  // ---- reserve ----

  #[tokio::test]
  async fn reserve_without_cache_size_stores_none() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();
    assert_eq!(find_upload(&ctx.db, id).await.unwrap().size, None);
  }

  #[tokio::test]
  async fn reserve_with_cache_size_minus_one_stores_none() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", Some(-1)).await.unwrap();
    assert_eq!(find_upload(&ctx.db, id).await.unwrap().size, None);
  }

  #[tokio::test]
  async fn reserve_with_non_negative_cache_size_stores_it() {
    let ctx = ctx().await;
    let zero = do_reserve(&ctx, "zero", Some(0)).await.unwrap();
    let some = do_reserve(&ctx, "some", Some(1234)).await.unwrap();
    assert_eq!(find_upload(&ctx.db, zero).await.unwrap().size, Some(0));
    assert_eq!(find_upload(&ctx.db, some).await.unwrap().size, Some(1234));
  }

  #[tokio::test]
  async fn reserve_with_cache_size_below_minus_one_is_bad_request() {
    let ctx = ctx().await;
    let err = do_reserve(&ctx, "key", Some(-2)).await.unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(
      cache_upload::Entity::find()
        .all(&ctx.db.0)
        .await
        .unwrap()
        .is_empty()
    );
  }

  #[tokio::test]
  async fn reserve_lowercases_key_and_stores_auth_and_s3_upload_id() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "Linux-Cargo-ABC", None).await.unwrap();

    let upload = find_upload(&ctx.db, id).await.unwrap();
    assert_eq!(upload.key, "linux-cargo-abc");
    assert_eq!(upload.version, "v1");
    assert_eq!(upload.repo, REPO);
    assert_eq!(upload.write_isolation_key, WIK);
    assert!(upload.s3_upload_id.is_some_and(|id| !id.is_empty()));
  }

  // ---- upload_chunk ----

  #[tokio::test]
  async fn upload_chunk_without_byte_range_is_bad_request() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    let err = upload_chunk(
      auth(WIK),
      ctx.storage.clone(),
      ctx.db.clone(),
      TypedHeader(ContentRange::unsatisfied_bytes(4)),
      Path(UploadChunkPath { id }),
      Bytes::from_static(b"data"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn upload_chunk_with_empty_body_is_bad_request() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    let err = upload_chunk(
      auth(WIK),
      ctx.storage.clone(),
      ctx.db.clone(),
      TypedHeader(ContentRange::bytes(0..1, None).unwrap()),
      Path(UploadChunkPath { id }),
      Bytes::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(parts_of(&ctx.db, id).await.is_empty());
  }

  #[tokio::test]
  async fn upload_chunk_for_unknown_upload_is_bad_request() {
    let ctx = ctx().await;
    let err = do_upload(&ctx, WIK, 42, 0, Bytes::from_static(b"data"))
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn upload_chunk_with_other_write_isolation_key_is_forbidden() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    let err = do_upload(&ctx, "other", id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
    assert!(parts_of(&ctx.db, id).await.is_empty());
  }

  #[tokio::test]
  async fn upload_chunk_from_other_repo_is_forbidden() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    let err = upload_chunk(
      Auth {
        repo: "other/repo".into(),
        write_isolation_key: WIK.into(),
      },
      ctx.storage.clone(),
      ctx.db.clone(),
      TypedHeader(ContentRange::bytes(0..4, None).unwrap()),
      Path(UploadChunkPath { id }),
      Bytes::from_static(b"data"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
    assert!(parts_of(&ctx.db, id).await.is_empty());
  }

  #[tokio::test]
  async fn upload_chunk_without_s3_upload_id_is_internal_error() {
    let ctx = ctx().await;
    let upload = insert_upload(&ctx.db, REPO, "key", "v1", WIK, now()).await;

    let err = do_upload(&ctx, WIK, upload.id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
  }

  #[tokio::test]
  async fn upload_chunk_computes_part_number_from_start_and_length() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    do_upload(&ctx, WIK, id, 8, Bytes::from_static(b"data"))
      .await
      .unwrap();

    let parts = parts_of(&ctx.db, id).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].part_number, 3);
    assert_eq!(parts[0].start_byte, 8);
    assert_eq!(parts[0].size, 4);
    assert!(parts[0].e_tag.is_some());
  }

  #[tokio::test]
  async fn upload_chunk_twice_at_same_start_overwrites_the_part() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"old!"))
      .await
      .unwrap();
    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"new"))
      .await
      .unwrap();

    let parts = parts_of(&ctx.db, id).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].size, 3);

    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;
    do_commit(&ctx, WIK, id).await.unwrap();
    assert_eq!(read(&ctx.storage, file_id).await, b"new");
  }

  #[tokio::test]
  async fn upload_chunk_refreshes_created_at() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();

    let old = now() - Duration::hours(1);
    let mut model = find_upload(&ctx.db, id).await.unwrap().into_active_model();
    model.created_at = Set(old);
    model.update(&ctx.db.0).await.unwrap();

    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap();

    assert!(find_upload(&ctx.db, id).await.unwrap().created_at > old);
  }

  // ---- commit ----

  #[tokio::test]
  async fn commit_creates_entry_removes_upload_and_stores_file() {
    let ctx = ctx().await;
    let size = 2 * MIN_PART as i64;
    let id = do_reserve(&ctx, "Key", Some(size)).await.unwrap();
    let upload = find_upload(&ctx.db, id).await.unwrap();

    // Chunks may arrive out of order.
    do_upload(&ctx, WIK, id, MIN_PART as u64, big_chunk(b'b'))
      .await
      .unwrap();
    do_upload(&ctx, WIK, id, 0, big_chunk(b'a')).await.unwrap();
    do_commit(&ctx, WIK, id).await.unwrap();

    let entry = cache_entry::Entity::find_by_id(id)
      .one(&ctx.db.0)
      .await
      .unwrap()
      .unwrap();
    assert_eq!(entry.key, "key");
    assert_eq!(entry.version, "v1");
    assert_eq!(entry.repo, REPO);
    assert_eq!(entry.write_isolation_key, WIK);
    assert_eq!(entry.size, size);
    assert_eq!(entry.file_id, upload.file_id);

    assert!(find_upload(&ctx.db, id).await.is_none());
    assert!(parts_of(&ctx.db, id).await.is_empty());
    assert!(
      cache_entry_pending::Entity::find()
        .all(&ctx.db.0)
        .await
        .unwrap()
        .is_empty()
    );

    let mut expected = big_chunk(b'a').to_vec();
    expected.extend_from_slice(&big_chunk(b'b'));
    assert_eq!(read(&ctx.storage, upload.file_id).await, expected);
  }

  async fn has_tail(ctx: &Ctx, file_id: uuid::Uuid) -> bool {
    ctx.storage.exists(&tail_path(file_id)).await.unwrap()
  }

  fn with_tail(chunks: &[u8]) -> Vec<u8> {
    let mut expected: Vec<u8> = chunks.iter().flat_map(|b| big_chunk(*b)).collect();
    expected.extend_from_slice(b"tail");
    expected
  }

  /// Regression: real clients (actions/cache) send equally sized chunks except
  /// a smaller last one. `start / len + 1` numbered that part far past the
  /// others, but object_store's `complete_multipart` numbers parts 1..n, so S3
  /// rejected the commit.
  #[tokio::test]
  async fn commit_with_smaller_last_chunk_succeeds() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", Some(MIN_PART as i64 + 4))
      .await
      .unwrap();
    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;

    do_upload(&ctx, WIK, id, 0, big_chunk(b'a')).await.unwrap();
    do_upload(&ctx, WIK, id, MIN_PART as u64, Bytes::from_static(b"tail"))
      .await
      .unwrap();

    let mut parts = parts_of(&ctx.db, id).await;
    parts.sort_by_key(|p| p.start_byte);
    assert_eq!(parts[1].part_number, 2);
    assert!(parts[1].e_tag.is_some());
    assert!(!has_tail(&ctx, file_id).await);

    do_commit(&ctx, WIK, id).await.unwrap();
    assert_eq!(read(&ctx.storage, file_id).await, with_tail(b"a"));
  }

  #[tokio::test]
  async fn tail_arriving_first_is_parked_until_commit() {
    let ctx = ctx().await;
    let start = 2 * MIN_PART as u64;
    let id = do_reserve(&ctx, "key", Some(start as i64 + 4))
      .await
      .unwrap();
    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;

    do_upload(&ctx, WIK, id, start, Bytes::from_static(b"tail"))
      .await
      .unwrap();

    let parts = parts_of(&ctx.db, id).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].part_number, 0);
    assert_eq!(parts[0].e_tag, None);
    assert!(has_tail(&ctx, file_id).await);

    do_upload(&ctx, WIK, id, MIN_PART as u64, big_chunk(b'b'))
      .await
      .unwrap();
    do_upload(&ctx, WIK, id, 0, big_chunk(b'a')).await.unwrap();
    do_commit(&ctx, WIK, id).await.unwrap();

    assert_eq!(read(&ctx.storage, file_id).await, with_tail(b"ab"));
    assert!(!has_tail(&ctx, file_id).await);
  }

  #[tokio::test]
  async fn parked_tail_retried_after_chunk_size_is_known_is_uploaded() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", Some(MIN_PART as i64 + 4))
      .await
      .unwrap();
    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;

    do_upload(&ctx, WIK, id, MIN_PART as u64, Bytes::from_static(b"tail"))
      .await
      .unwrap();
    do_upload(&ctx, WIK, id, 0, big_chunk(b'a')).await.unwrap();
    do_upload(&ctx, WIK, id, MIN_PART as u64, Bytes::from_static(b"tail"))
      .await
      .unwrap();

    let mut parts = parts_of(&ctx.db, id).await;
    parts.sort_by_key(|p| p.start_byte);
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[1].part_number, 2);
    assert!(parts[1].e_tag.is_some());

    do_commit(&ctx, WIK, id).await.unwrap();
    assert_eq!(read(&ctx.storage, file_id).await, with_tail(b"a"));
    assert!(!has_tail(&ctx, file_id).await);
  }

  #[tokio::test]
  async fn commit_with_only_parked_tail_is_bad_request() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", Some(MIN_PART as i64 + 4))
      .await
      .unwrap();

    do_upload(&ctx, WIK, id, MIN_PART as u64, Bytes::from_static(b"tail"))
      .await
      .unwrap();

    let err = do_commit(&ctx, WIK, id).await.unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(find_upload(&ctx.db, id).await.is_some());
  }

  #[tokio::test]
  async fn commit_without_reserved_size_with_uniform_chunks_succeeds() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();
    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;

    do_upload(&ctx, WIK, id, MIN_PART as u64, big_chunk(b'b'))
      .await
      .unwrap();
    do_upload(&ctx, WIK, id, 0, big_chunk(b'a')).await.unwrap();
    do_commit(&ctx, WIK, id).await.unwrap();

    let mut expected = big_chunk(b'a').to_vec();
    expected.extend_from_slice(&big_chunk(b'b'));
    assert_eq!(read(&ctx.storage, file_id).await, expected);
  }

  #[tokio::test]
  async fn commit_with_size_mismatch_is_bad_request() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", Some(10)).await.unwrap();
    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap();

    let err = do_commit(&ctx, WIK, id).await.unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(find_upload(&ctx.db, id).await.is_some());
    assert!(
      cache_entry::Entity::find()
        .all(&ctx.db.0)
        .await
        .unwrap()
        .is_empty()
    );
  }

  #[tokio::test]
  async fn commit_without_reserved_size_accepts_any_size() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();
    let file_id = find_upload(&ctx.db, id).await.unwrap().file_id;
    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap();

    do_commit(&ctx, WIK, id).await.unwrap();

    let entry = cache_entry::Entity::find_by_id(id)
      .one(&ctx.db.0)
      .await
      .unwrap()
      .unwrap();
    assert_eq!(entry.size, 4);
    assert_eq!(read(&ctx.storage, file_id).await, b"data");
  }

  #[tokio::test]
  async fn commit_with_other_write_isolation_key_is_forbidden() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();
    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap();

    let err = do_commit(&ctx, "other", id).await.unwrap_err();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
    assert!(find_upload(&ctx.db, id).await.is_some());
  }

  #[tokio::test]
  async fn commit_from_other_repo_is_forbidden() {
    let ctx = ctx().await;
    let id = do_reserve(&ctx, "key", None).await.unwrap();
    do_upload(&ctx, WIK, id, 0, Bytes::from_static(b"data"))
      .await
      .unwrap();

    let err = commit(
      Auth {
        repo: "other/repo".into(),
        write_isolation_key: WIK.into(),
      },
      ctx.storage.clone(),
      ctx.db.clone(),
      Path(UploadChunkPath { id }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
    assert!(find_upload(&ctx.db, id).await.is_some());
  }

  #[tokio::test]
  async fn commit_for_unknown_upload_is_bad_request() {
    let ctx = ctx().await;
    let err = do_commit(&ctx, WIK, 42).await.unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn commit_replacing_existing_key_schedules_old_file_for_cleanup() {
    let ctx = ctx().await;

    let first = do_reserve(&ctx, "key", None).await.unwrap();
    let old_file = find_upload(&ctx.db, first).await.unwrap().file_id;
    do_upload(&ctx, WIK, first, 0, Bytes::from_static(b"old"))
      .await
      .unwrap();
    do_commit(&ctx, WIK, first).await.unwrap();

    let second = do_reserve(&ctx, "key", None).await.unwrap();
    let new_file = find_upload(&ctx.db, second).await.unwrap().file_id;
    do_upload(&ctx, WIK, second, 0, Bytes::from_static(b"new"))
      .await
      .unwrap();
    do_commit(&ctx, WIK, second).await.unwrap();

    let entries = cache_entry::Entity::find().all(&ctx.db.0).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].file_id, new_file);

    let cleanups = cache_cleanup::Entity::find().all(&ctx.db.0).await.unwrap();
    assert_eq!(cleanups.len(), 1);
    assert_eq!(cleanups[0].file_id, old_file);
  }

  // ---- upload_id ----

  #[tokio::test]
  async fn upload_id_without_s3_upload_id_is_internal_error() {
    let db = test_db().await;
    let mut upload = insert_upload(&db, REPO, "key", "v1", WIK, now()).await;

    let err = upload_id(&upload).unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

    upload.s3_upload_id = Some("abc".into());
    assert_eq!(upload_id(&upload).unwrap(), "abc");
  }

  // ---- clean ----

  #[tokio::test]
  async fn clean_is_a_no_op() {
    assert!(clean(auth(WIK)).await.is_ok());
  }
}
