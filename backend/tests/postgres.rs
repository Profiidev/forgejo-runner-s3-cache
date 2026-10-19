//! Cache flows against a real postgres (testcontainers) instead of sqlite.
mod common;

use common::{MIN_PART, TestServer, payload};
use reqwest::StatusCode;

const VERSION: &str = "v1";

#[tokio::test]
async fn pg_full_flow() {
  let server = TestServer::start_pg().await;
  let data = payload(2 * MIN_PART, 1);
  let id = server.store("key", VERSION, &data, MIN_PART).await;

  let hit = server.find_hit(&["key"], VERSION).await;
  assert_eq!(hit["cacheKey"], "key");
  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

#[tokio::test]
async fn pg_overwrite_serves_new_content() {
  let server = TestServer::start_pg().await;
  let old = payload(100, 2);
  let new = payload(200, 3);
  server.store("same", VERSION, &old, old.len()).await;
  let new_id = server.store("same", VERSION, &new, new.len()).await;

  let hit = server.find_hit(&["same"], VERSION).await;
  assert_eq!(
    hit["archiveLocation"],
    format!("http://cache.local/7/_apis/artifactcache/artifacts/{new_id}")
  );
  assert_eq!(
    server.download(new_id, None).await.bytes().await.unwrap(),
    new
  );
}

#[tokio::test]
async fn pg_restore_key_matches_by_prefix() {
  let server = TestServer::start_pg().await;
  let data = payload(100, 4);
  server
    .store("deps-abc123", VERSION, &data, data.len())
    .await;

  let hit = server.find_hit(&["deps-zzz", "deps-"], VERSION).await;
  assert_eq!(hit["cacheKey"], "deps-abc123");

  let resp = server.find(&["deps_"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}
