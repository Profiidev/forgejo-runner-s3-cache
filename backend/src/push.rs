use axum::{Json, Router, routing::post};
use centaurus::{bail, db::init::Connection, error::Result, storage::FileStorage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{auth::Auth, db::DBTrait, storage::StorageExt};

pub fn router() -> Router {
  Router::new().route("/caches", post(reserve))
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
      req.key,
      req.version,
      req.cache_size,
      auth.repo,
      auth.write_isolation_key,
      upload_id,
    )
    .await?;

  Ok(Json(ReserveResponse { cache_id }))
}
