//! Every cache route must reject requests that are not signed correctly.
mod common;

use common::{REPO, SECRET, TestServer, sign};
use reqwest::{Method, RequestBuilder, StatusCode};

const ROUTES: [(Method, &str); 6] = [
  (Method::POST, "/caches"),
  (Method::PATCH, "/caches/1"),
  (Method::POST, "/caches/1"),
  (Method::POST, "/clean"),
  (Method::GET, "/cache?keys=a&version=v"),
  (Method::GET, "/artifacts/1"),
];

async fn assert_all_routes(
  server: &TestServer,
  expected: StatusCode,
  build: impl Fn(RequestBuilder) -> RequestBuilder,
) {
  for (method, path) in ROUTES {
    let resp = build(server.raw(method.clone(), path))
      .send()
      .await
      .unwrap();
    assert_eq!(resp.status(), expected, "{method} {path}");
  }
}

fn now() -> i64 {
  chrono::Utc::now().timestamp()
}

#[tokio::test]
async fn unsigned_requests_are_rejected() {
  let server = TestServer::start().await;
  assert_all_routes(&server, StatusCode::BAD_REQUEST, |req| req).await;
}

#[tokio::test]
async fn bad_mac_is_forbidden() {
  let server = TestServer::start().await;
  assert_all_routes(&server, StatusCode::FORBIDDEN, |req| {
    sign(req, "wrong-secret", REPO, "1", &now().to_string(), "")
  })
  .await;
}

#[tokio::test]
async fn future_timestamp_is_forbidden() {
  let server = TestServer::start().await;
  assert_all_routes(&server, StatusCode::FORBIDDEN, |req| {
    sign(req, SECRET, REPO, "1", &(now() + 3600).to_string(), "")
  })
  .await;
}
