//! End-to-end cache flows against sqlite + S3Mock.
mod common;

use common::{MIN_PART, TestServer, payload};
use reqwest::StatusCode;

const VERSION: &str = "v1";

#[tokio::test]
async fn uniform_chunks_round_trip() {
  let server = TestServer::start().await;
  let data = payload(2 * MIN_PART, 1);
  let id = server
    .store("Linux-Node-20", VERSION, &data, MIN_PART)
    .await;

  let hit = server.find_hit(&["linux-node-20"], VERSION).await;
  assert_eq!(hit["result"], "hit");
  assert_eq!(hit["cacheKey"], "linux-node-20");
  assert_eq!(
    hit["archiveLocation"],
    format!("http://cache.local/7/_apis/artifactcache/artifacts/{id}")
  );

  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

/// Regression: actions/cache sends equal chunks plus a smaller last one;
/// push.rs used to number parts `start / len + 1`, which broke the S3 commit
/// for that last part.
#[tokio::test]
async fn smaller_tail_chunk_round_trips() {
  let server = TestServer::start().await;
  let data = payload(MIN_PART + 1024, 2);
  let id = server.store("tail", VERSION, &data, MIN_PART).await;

  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

/// Chunks are uploaded concurrently, so the tail may arrive before any chunk
/// that reveals the chunk size.
#[tokio::test]
async fn tail_chunk_uploaded_first_round_trips() {
  let server = TestServer::start().await;
  let data = payload(2 * MIN_PART + 1024, 11);
  let id = server
    .reserve_id("tail-first", VERSION, data.len() as i64)
    .await;

  let mut chunks: Vec<_> = data.chunks(MIN_PART).enumerate().collect();
  chunks.reverse();
  for (i, part) in chunks {
    let resp = server.upload_chunk(id, i * MIN_PART, part).await;
    assert_eq!(resp.status(), StatusCode::OK, "upload chunk {i}");
  }
  assert_eq!(server.commit(id).await.status(), StatusCode::OK, "commit");

  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

#[tokio::test]
async fn single_chunk_round_trips() {
  let server = TestServer::start().await;
  let data = payload(1000, 3);
  let id = server.store("small", VERSION, &data, data.len()).await;

  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

/// Regression: the inclusive range end used to be incremented twice, returning
/// one byte too many.
#[tokio::test]
async fn range_download_returns_slice() {
  let server = TestServer::start().await;
  let data = payload(1000, 4);
  let id = server.store("range", VERSION, &data, data.len()).await;

  let resp = server.download(id, Some("bytes=10-19")).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data[10..20]);

  let resp = server.download(id, Some("bytes=990-")).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data[990..]);
}

#[tokio::test]
async fn overwriting_key_serves_new_content() {
  let server = TestServer::start().await;
  let old = payload(100, 5);
  let new = payload(200, 6);
  server.store("same", VERSION, &old, old.len()).await;
  let new_id = server.store("same", VERSION, &new, new.len()).await;

  let hit = server.find_hit(&["same"], VERSION).await;
  assert_eq!(
    hit["archiveLocation"],
    format!("http://cache.local/7/_apis/artifactcache/artifacts/{new_id}")
  );
  let resp = server.download(new_id, None).await;
  assert_eq!(resp.bytes().await.unwrap(), new);
}

#[tokio::test]
async fn restore_key_matches_by_prefix() {
  let server = TestServer::start().await;
  let data = payload(100, 7);
  server
    .store("deps-abc123", VERSION, &data, data.len())
    .await;

  let hit = server.find_hit(&["deps-zzz", "deps-"], VERSION).await;
  assert_eq!(hit["cacheKey"], "deps-abc123");
}

#[tokio::test]
async fn like_wildcards_in_restore_key_are_literal() {
  let server = TestServer::start().await;
  let data = payload(100, 8);
  server.store("deps-abc", VERSION, &data, data.len()).await;

  let resp = server.find(&["deps_", "%"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn other_version_misses() {
  let server = TestServer::start().await;
  let data = payload(100, 9);
  server.store("key", VERSION, &data, data.len()).await;

  let resp = server.find(&["key"], "v2").await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn other_repo_cannot_see_cache() {
  let mut server = TestServer::start().await;
  let data = payload(100, 10);
  server.store("key", VERSION, &data, data.len()).await;

  server.as_repo("other/repo");
  let resp = server.find(&["key"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn missing_cache_returns_no_content() {
  let server = TestServer::start().await;
  let resp = server.find(&["nothing"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn download_of_unknown_id_is_not_found() {
  let server = TestServer::start().await;
  let resp = server.download(9999, None).await;
  assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pull_request_reads_base_cache() {
  let mut server = TestServer::start().await;
  let data = payload(100, 11);
  let id = server.store("base", VERSION, &data, data.len()).await;

  server.with_wik("refs/pull/1/head");
  let hit = server.find_hit(&["base"], VERSION).await;
  assert_eq!(hit["cacheKey"], "base");
  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::OK);
  assert_eq!(resp.bytes().await.unwrap(), data);
}

#[tokio::test]
async fn base_cannot_read_pull_request_cache() {
  let mut server = TestServer::start().await;
  server.with_wik("refs/pull/1/head");
  let data = payload(100, 12);
  let id = server.store("pr", VERSION, &data, data.len()).await;

  server.with_wik("");
  let resp = server.find(&["pr"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::FORBIDDEN);

  server.with_wik("refs/pull/2/head");
  let resp = server.download(id, None).await;
  assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn pull_request_prefers_own_cache() {
  let mut server = TestServer::start().await;
  let base = payload(100, 13);
  server.store("key", VERSION, &base, base.len()).await;
  server.with_wik("refs/pull/1/head");
  let pr = payload(100, 14);
  let pr_id = server.store("key", VERSION, &pr, pr.len()).await;

  let hit = server.find_hit(&["key"], VERSION).await;
  assert_eq!(
    hit["archiveLocation"],
    format!("http://cache.local/7/_apis/artifactcache/artifacts/{pr_id}")
  );
}

#[tokio::test]
async fn other_isolation_key_cannot_write_reservation() {
  let mut server = TestServer::start().await;
  let id = server.reserve_id("key", VERSION, 10).await;

  server.with_wik("refs/pull/1/head");
  let resp = server.upload_chunk(id, 0, &payload(10, 15)).await;
  assert_eq!(resp.status(), StatusCode::FORBIDDEN);
  assert_eq!(server.commit(id).await.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn commit_with_wrong_size_fails() {
  let server = TestServer::start().await;
  let id = server.reserve_id("key", VERSION, 200).await;
  let resp = server.upload_chunk(id, 0, &payload(100, 16)).await;
  assert_eq!(resp.status(), StatusCode::OK);

  assert_eq!(server.commit(id).await.status(), StatusCode::BAD_REQUEST);
  let resp = server.find(&["key"], VERSION).await;
  assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn unknown_size_reservation_commits() {
  let server = TestServer::start().await;
  let data = payload(100, 17);
  let id = server.reserve_id("key", VERSION, -1).await;
  assert_eq!(
    server.upload_chunk(id, 0, &data).await.status(),
    StatusCode::OK
  );
  assert_eq!(server.commit(id).await.status(), StatusCode::OK);

  let resp = server.download(id, None).await;
  assert_eq!(resp.bytes().await.unwrap(), data);
}

#[tokio::test]
async fn negative_reservation_size_is_rejected() {
  let server = TestServer::start().await;
  let resp = server.reserve("key", VERSION, -2).await;
  assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn clean_is_ok() {
  let server = TestServer::start().await;
  let resp = server
    .request(reqwest::Method::POST, "/clean")
    .send()
    .await
    .unwrap();
  assert_eq!(resp.status(), StatusCode::OK);
}
