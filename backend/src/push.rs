use axum::{
  Json, Router,
  body::Bytes,
  extract::Path,
  routing::{patch, post},
};
use axum_extra::{TypedHeader, headers::ContentRange};
use centaurus::{bail, db::init::Connection, error::Result, storage::FileStorage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{auth::Auth, db::DBTrait, storage::StorageExt};

pub fn router() -> Router {
  Router::new()
    .route("/caches", post(reserve))
    .route("/caches/{id}", patch(upload_chunk))
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
  cache_id: Uuid,
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

  let cache_id = Uuid::new_v4();

  let upload_id = storage
    .create_multipart_upload(&cache_id.to_string())
    .await?;

  db.cache_upload()
    .reserve(
      cache_id,
      req.key.to_lowercase(),
      req.version,
      req.cache_size,
      auth.repo,
      auth.write_isolation_key,
      upload_id,
    )
    .await?;

  Ok(Json(ReserveResponse { cache_id }))
}

#[derive(Deserialize)]
struct UploadChunkPath {
  id: Uuid,
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

  let upload = db.cache_upload().find(path.id).await?;

  if auth.write_isolation_key != upload.write_isolation_key {
    bail!(FORBIDDEN, "Write isolation key does not match");
  }

  let chunk = db
    .cache_upload()
    .chunk(path.id, req.len() as i64, start as i64)
    .await?;

  let etag = storage
    .upload_part(
      &upload.id.to_string(),
      upload.s3_upload_id.as_deref(),
      chunk.part_number,
      req,
    )
    .await?;

  if let Some(etag) = etag {
    db.cache_upload().update_etag(chunk.id, etag).await?;
  }

  Ok(())
}

async fn commit(
  auth: Auth,
  storage: FileStorage,
  db: Connection,
  Path(path): Path<UploadChunkPath>,
) -> Result<()> {
  let upload = db.cache_upload().find(path.id).await?;

  if auth.write_isolation_key != upload.write_isolation_key {
    bail!(FORBIDDEN, "Write isolation key does not match");
  }

  let parts = db.cache_upload().parts(upload.id).await?;

  let size: i64 = parts.iter().map(|part| part.size).sum();

  let cache = db.cache_entry().create_cache(upload.clone(), size).await?;

  storage
    .complete_multipart_upload(
      &upload.id.to_string(),
      upload.s3_upload_id.as_deref(),
      parts,
    )
    .await?;

  db.cache_entry().complete(cache).await?;

  Ok(())
}

async fn clean(_auth: Auth) -> Result<()> {
  Ok(())
}
