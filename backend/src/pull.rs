use axum::{
  Json, Router,
  body::Body,
  extract::{Path, Query},
  routing::get,
};
use centaurus::{bail, db::init::Connection, error::Result, storage::FileStorage};
use http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::{auth::Auth, db::DBTrait};

pub fn router() -> Router {
  Router::new()
    .route("/cache", get(find))
    .route("/artifacts/{id}", get(download))
}

#[derive(Deserialize)]
struct FindQuery {
  keys: String,
  version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FindResult {
  cache_key: String,
  result: String,
  archive_location: String,
}

async fn find(
  auth: Auth,
  Query(query): Query<FindQuery>,
  db: Connection,
  headers: HeaderMap,
) -> Result<Json<FindResult>> {
  let keys = query
    .keys
    .split(',')
    .map(|s| s.trim().to_lowercase())
    .filter(|s| !s.is_empty())
    .collect::<Vec<_>>();

  let Some(entry) = db
    .cache_entry()
    .find_entry(&keys, query.version, auth)
    .await?
  else {
    bail!(NO_CONTENT, "Cache entry not found");
  };

  let host = headers
    .get("Forgejo-Cache-Host")
    .and_then(|v| v.to_str().ok())
    .unwrap_or_default();

  let run_id = headers
    .get("Forgejo-Cache-RunId")
    .and_then(|v| v.to_str().ok())
    .unwrap_or_default();

  Ok(Json(FindResult {
    cache_key: entry.key,
    result: "hit".into(),
    archive_location: format!(
      "{}/{}/_apis/artifactcache/artifacts/{}",
      host, run_id, entry.id
    ),
  }))
}

#[derive(Deserialize)]
struct DownloadPath {
  id: i32,
}

async fn download(
  auth: Auth,
  db: Connection,
  storage: FileStorage,
  Path(path): Path<DownloadPath>,
) -> Result<Body> {
  let Some(entry) = db.cache_entry().find_by_id(path.id).await? else {
    bail!(NOT_FOUND, "Cache entry not found");
  };

  if entry.write_isolation_key != auth.write_isolation_key && !entry.write_isolation_key.is_empty()
  {
    bail!(FORBIDDEN, "Cache entry not found");
  }

  let body = storage.get_file(&entry.id.to_string(), None).await?;

  db.cache_entry().update_used_at(path.id).await?;

  Ok(body)
}
