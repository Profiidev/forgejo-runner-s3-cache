use std::ops::Bound;

use axum::{
  Json, Router,
  extract::{Path, Query},
  response::IntoResponse,
  routing::get,
};
use axum_extra::{TypedHeader, headers::Range};
use centaurus::{bail, db::init::Connection, error::Result, storage::FileStorage};
use http::{HeaderMap, StatusCode};
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
  #[serde(skip_serializing_if = "Option::is_none")]
  cache_key: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  result: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  archive_location: Option<String>,
}

async fn find(
  auth: Auth,
  Query(query): Query<FindQuery>,
  db: Connection,
  headers: HeaderMap,
  storage: FileStorage,
) -> Result<impl IntoResponse> {
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
    return Ok((
      StatusCode::NO_CONTENT,
      Json(FindResult {
        cache_key: None,
        result: None,
        archive_location: None,
      }),
    ));
  };

  if !storage.exists(&entry.id.to_string()).await? {
    return Ok((
      StatusCode::NO_CONTENT,
      Json(FindResult {
        cache_key: None,
        result: None,
        archive_location: None,
      }),
    ));
  }

  let host = headers
    .get("Forgejo-Cache-Host")
    .and_then(|v| v.to_str().ok())
    .unwrap_or_default();

  let run_id = headers
    .get("Forgejo-Cache-RunId")
    .and_then(|v| v.to_str().ok())
    .unwrap_or_default();

  Ok((
    StatusCode::OK,
    Json(FindResult {
      cache_key: Some(entry.key),
      result: Some("hit".into()),
      archive_location: Some(format!(
        "{}/{}/_apis/artifactcache/artifacts/{}",
        host, run_id, entry.id
      )),
    }),
  ))
}

#[derive(Deserialize)]
struct DownloadPath {
  id: i32,
}

async fn download(
  auth: Auth,
  db: Connection,
  storage: FileStorage,
  range: Option<TypedHeader<Range>>,
  Path(path): Path<DownloadPath>,
) -> Result<impl IntoResponse> {
  let Some(entry) = db.cache_entry().find_by_id(path.id).await? else {
    bail!(NOT_FOUND, "Cache entry not found");
  };

  if entry.write_isolation_key != auth.write_isolation_key && !entry.write_isolation_key.is_empty()
  {
    bail!(FORBIDDEN, "Cache entry not found");
  }

  let range = if let Some(TypedHeader(range)) = range
    && let Some((start, end)) = range.satisfiable_ranges(entry.size as u64).next()
  {
    let start = match start {
      Bound::Included(s) => s,
      Bound::Excluded(s) => s + 1,
      Bound::Unbounded => 0,
    };
    let end = match end {
      Bound::Included(e) => e + 1,
      Bound::Excluded(e) => e,
      Bound::Unbounded => entry.size as u64,
    };
    Some((start, end))
  } else {
    None
  };

  let body = storage.get_file(&entry.id.to_string(), range).await?;

  db.cache_entry().update_used_at(path.id).await?;

  Ok(body)
}
