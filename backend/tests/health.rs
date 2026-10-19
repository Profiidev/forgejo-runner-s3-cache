mod common;

use common::TestServer;
use reqwest::StatusCode;

#[tokio::test]
async fn health_is_ok() {
  let server = TestServer::start().await;
  let resp = reqwest::get(server.url("/api/health")).await.unwrap();
  assert_eq!(resp.status(), StatusCode::OK);
}
