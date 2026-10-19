use std::ops::Bound;

use axum::{
  Json, Router,
  extract::{Path, Query},
  response::IntoResponse,
  routing::get,
};
use axum_extra::{TypedHeader, headers::Range};
use centaurus::{
  bail,
  db::init::Connection,
  error::Result,
  storage::{FileStorage, StoragePath},
};
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

  if !storage
    .exists(&StoragePath::from(entry.file_id.to_string()))
    .await?
  {
    db.cache_entry().delete_by_id(entry.id).await?;

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

  if entry.repo != auth.repo
    || (entry.write_isolation_key != auth.write_isolation_key
      && !entry.write_isolation_key.is_empty())
  {
    bail!(FORBIDDEN, "Cache entry not found");
  }

  // `get_file` takes an inclusive end. Unsatisfiable ranges serve the full body.
  let size = entry.size as u64;
  let range = range
    .and_then(|TypedHeader(range)| range.satisfiable_ranges(size).next())
    .and_then(|(start, end)| {
      let start = match start {
        Bound::Included(s) => s,
        Bound::Excluded(s) => s + 1,
        Bound::Unbounded => 0,
      };
      let end = match end {
        Bound::Included(e) => e,
        Bound::Excluded(e) => e.checked_sub(1)?,
        Bound::Unbounded => size.checked_sub(1)?,
      }
      .min(size.checked_sub(1)?);
      (start <= end).then_some((start, end))
    });

  let body = storage
    .get_file(&StoragePath::from(entry.file_id.to_string()), range)
    .await?;

  db.cache_entry().update_used_at(path.id).await?;

  Ok(body)
}

#[cfg(test)]
mod test {
  use axum::{
    extract::{Path, Query},
    response::{IntoResponse, Response},
  };
  use axum_extra::{
    TypedHeader,
    headers::{Header, Range},
  };
  use centaurus::{
    db::init::Connection,
    storage::{FileStorage, StoragePath},
  };
  use chrono::Duration;
  use entity::cache_entry;
  use http::{HeaderMap, HeaderValue, StatusCode};
  use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set};
  use serde_json::{Value, json};
  use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

  use super::{DownloadPath, FindQuery, download, find};
  use crate::{
    auth::Auth,
    db::test::{insert_entry, now, test_db},
    storage::test::init_s3_storage,
  };

  const REPO: &str = "owner/repo";
  const WIK: &str = "refs/heads/main";
  const DATA: &[u8] = b"0123456789";

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

  /// Committed entry whose object holds `DATA`, last used `used_at`.
  async fn entry_with_file(ctx: &Ctx, key: &str, wik: &str) -> cache_entry::Model {
    let old = now() - Duration::hours(1);
    let entry = insert_entry(&ctx.db, REPO, key, "v1", wik, old, old).await;
    let mut model = entry.into_active_model();
    model.size = Set(DATA.len() as i64);
    let entry = model.update(&ctx.db.0).await.unwrap();

    ctx
      .storage
      .save_file(&mut &DATA[..], StoragePath::from(entry.file_id.to_string()))
      .await
      .unwrap();
    entry
  }

  async fn body_bytes(res: Response) -> Vec<u8> {
    axum::body::to_bytes(res.into_body(), usize::MAX)
      .await
      .unwrap()
      .to_vec()
  }

  async fn do_find(ctx: &Ctx, wik: &str, keys: &str, headers: HeaderMap) -> (StatusCode, Value) {
    let res = find(
      auth(wik),
      Query(FindQuery {
        keys: keys.into(),
        version: "v1".into(),
      }),
      ctx.db.clone(),
      headers,
      ctx.storage.clone(),
    )
    .await
    .unwrap()
    .into_response();
    let status = res.status();
    (
      status,
      serde_json::from_slice(&body_bytes(res).await).unwrap(),
    )
  }

  fn range(value: &'static str) -> Option<TypedHeader<Range>> {
    let value = HeaderValue::from_static(value);
    Some(TypedHeader(
      Range::decode(&mut std::iter::once(&value)).unwrap(),
    ))
  }

  async fn do_download(
    ctx: &Ctx,
    wik: &str,
    id: i32,
    range: Option<TypedHeader<Range>>,
  ) -> centaurus::error::Result<Vec<u8>> {
    let res = download(
      auth(wik),
      ctx.db.clone(),
      ctx.storage.clone(),
      range,
      Path(DownloadPath { id }),
    )
    .await?
    .into_response();
    assert_eq!(res.status(), StatusCode::OK);
    Ok(body_bytes(res).await)
  }

  // ---- find ----

  #[tokio::test]
  async fn find_hit_returns_archive_location_from_forwarded_headers() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let mut headers = HeaderMap::new();
    headers.insert(
      "Forgejo-Cache-Host",
      HeaderValue::from_static("http://cache:8080"),
    );
    headers.insert("Forgejo-Cache-RunId", HeaderValue::from_static("run-1"));

    let (status, body) = do_find(&ctx, WIK, "key", headers).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
      body,
      json!({
        "cacheKey": "key",
        "result": "hit",
        "archiveLocation": format!(
          "http://cache:8080/run-1/_apis/artifactcache/artifacts/{}",
          entry.id
        ),
      })
    );
  }

  #[tokio::test]
  async fn find_hit_without_forwarded_headers_leaves_host_and_run_empty() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let (status, body) = do_find(&ctx, WIK, "key", HeaderMap::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
      body["archiveLocation"],
      format!("//_apis/artifactcache/artifacts/{}", entry.id)
    );
  }

  #[tokio::test]
  async fn find_miss_is_no_content() {
    let ctx = ctx().await;
    entry_with_file(&ctx, "key", WIK).await;

    let (status, body) = do_find(&ctx, WIK, "other", HeaderMap::new()).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, json!({}));
  }

  #[tokio::test]
  async fn find_trims_and_lowercases_keys() {
    let ctx = ctx().await;
    entry_with_file(&ctx, "key", WIK).await;

    let (status, body) = do_find(&ctx, WIK, " missing , KEY ", HeaderMap::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cacheKey"], "key");
  }

  #[tokio::test]
  async fn find_ignores_empty_keys() {
    let ctx = ctx().await;
    entry_with_file(&ctx, "key", WIK).await;

    // An empty key would prefix-match every entry.
    let (status, _) = do_find(&ctx, WIK, ", ,missing,", HeaderMap::new()).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
  }

  #[tokio::test]
  async fn find_entry_with_missing_object_deletes_row_and_is_no_content() {
    let ctx = ctx().await;
    let entry = insert_entry(&ctx.db, REPO, "key", "v1", WIK, now(), now()).await;

    let (status, _) = do_find(&ctx, WIK, "key", HeaderMap::new()).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
      cache_entry::Entity::find_by_id(entry.id)
        .one(&ctx.db.0)
        .await
        .unwrap()
        .is_none()
    );
  }

  // ---- download ----

  #[tokio::test]
  async fn download_unknown_entry_is_not_found() {
    let ctx = ctx().await;
    let err = do_download(&ctx, WIK, 42, None).await.unwrap_err();
    assert_eq!(err.status, StatusCode::NOT_FOUND);
  }

  #[tokio::test]
  async fn download_with_other_write_isolation_key_is_forbidden() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let err = do_download(&ctx, "other", entry.id, None)
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn download_from_other_repo_is_forbidden() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", "").await;

    let err = download(
      Auth {
        repo: "other/repo".into(),
        write_isolation_key: String::new(),
      },
      ctx.db.clone(),
      ctx.storage.clone(),
      None,
      Path(DownloadPath { id: entry.id }),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.status, StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn download_entry_without_write_isolation_key_is_readable_by_any_key() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", "").await;

    assert_eq!(do_download(&ctx, "", entry.id, None).await.unwrap(), DATA);
    assert_eq!(do_download(&ctx, WIK, entry.id, None).await.unwrap(), DATA);
  }

  #[tokio::test]
  async fn download_without_range_returns_full_body_and_updates_used_at() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    assert_eq!(do_download(&ctx, WIK, entry.id, None).await.unwrap(), DATA);

    let updated = cache_entry::Entity::find_by_id(entry.id)
      .one(&ctx.db.0)
      .await
      .unwrap()
      .unwrap();
    assert!(updated.used_at > entry.used_at);
  }

  #[tokio::test]
  async fn download_with_bounded_range_returns_inclusive_slice() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let body = do_download(&ctx, WIK, entry.id, range("bytes=2-5"))
      .await
      .unwrap();
    assert_eq!(body, b"2345");
  }

  #[tokio::test]
  async fn download_with_range_end_past_size_is_clamped() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let body = do_download(&ctx, WIK, entry.id, range("bytes=8-100"))
      .await
      .unwrap();
    assert_eq!(body, b"89");
  }

  #[tokio::test]
  async fn download_with_open_ended_range_returns_rest() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let body = do_download(&ctx, WIK, entry.id, range("bytes=7-"))
      .await
      .unwrap();
    assert_eq!(body, b"789");
  }

  #[tokio::test]
  async fn download_with_suffix_range_returns_last_bytes() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    let body = do_download(&ctx, WIK, entry.id, range("bytes=-3"))
      .await
      .unwrap();
    assert_eq!(body, b"789");
  }

  #[tokio::test]
  async fn download_with_unsatisfiable_range_returns_full_body() {
    let ctx = ctx().await;
    let entry = entry_with_file(&ctx, "key", WIK).await;

    for value in ["bytes=-20", "bytes=10-", "bytes=50-60", "bytes=5-2"] {
      let body = do_download(&ctx, WIK, entry.id, range(value))
        .await
        .unwrap();
      assert_eq!(body, DATA, "{value}");
    }
  }
}
