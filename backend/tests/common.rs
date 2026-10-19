//! Shared harness for the backend integration tests.
//!
//! Every test boots a *real* server on an OS-assigned port (`PORT=0`) and
//! drives it over HTTP with `reqwest`. The app requires S3, so every server
//! gets its own throwaway S3Mock container (testcontainers, needs Docker).
//! [`TestServer::start`] uses an in-memory sqlite database,
//! [`TestServer::start_pg`] a throwaway postgres container.
//!
//! `cargo nextest` runs each test in its own process, so the process-global
//! environment configured here never leaks between tests.
#![allow(dead_code)]

use std::time::Duration;

use backend::App;
use hmac::{KeyInit, Mac};
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode};
use serde_json::{Value, json};
use sha2::Sha256;
use testcontainers_modules::{
  postgres::Postgres,
  testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
  },
};
use tokio::{spawn, time::sleep};

pub const SECRET: &str = "integration-secret";
pub const REPO: &str = "owner/repo";
pub const BUCKET: &str = "cache";
/// Smallest non-final S3 multipart part.
pub const MIN_PART: usize = 5 * 1024 * 1024;

/// A container the server depends on; kept alive as long as the server.
pub enum TestContainer {
  Postgres(ContainerAsync<Postgres>),
  S3(ContainerAsync<GenericImage>),
}

/// A running test server plus the identity requests are signed with.
pub struct TestServer {
  pub port: u16,
  client: Client,
  repo: String,
  write_isolation_key: String,
  containers: Vec<TestContainer>,
}

impl TestServer {
  /// Boot a fresh server with an empty in-memory sqlite database.
  pub async fn start() -> TestServer {
    let s3 = prepare_env().await;
    TestServer::boot(vec![s3]).await
  }

  /// Boot a fresh server backed by a throwaway postgres container.
  pub async fn start_pg() -> TestServer {
    let s3 = prepare_env().await;
    let container = Postgres::default()
      .with_tag("18-alpine")
      .start()
      .await
      .expect("start postgres container");
    let url = format!(
      "postgres://postgres:postgres@{}:{}/postgres",
      container.get_host().await.unwrap(),
      container.get_host_port_ipv4(5432).await.unwrap()
    );
    unsafe {
      std::env::set_var("DB_URL", url);
    }
    TestServer::boot(vec![s3, TestContainer::Postgres(container)]).await
  }

  async fn boot(containers: Vec<TestContainer>) -> TestServer {
    let app = App::new().await;
    let port = app.port();
    spawn(app.run());

    let client = Client::builder()
      .timeout(Duration::from_secs(30))
      .connect_timeout(Duration::from_secs(30))
      .build()
      .expect("build reqwest client");

    let server = TestServer {
      port,
      client,
      repo: REPO.into(),
      write_isolation_key: String::new(),
      containers,
    };

    for _ in 0..100 {
      if let Ok(resp) = server.client.get(server.url("/api/health")).send().await
        && resp.status().is_success()
      {
        return server;
      }
      sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not become ready in time");
  }

  /// Sign subsequent requests as `repo`.
  pub fn as_repo(&mut self, repo: &str) -> &mut Self {
    self.repo = repo.into();
    self
  }

  /// Sign subsequent requests with this write isolation key ("" = base branch).
  pub fn with_wik(&mut self, wik: &str) -> &mut Self {
    self.write_isolation_key = wik.into();
    self
  }

  pub fn url(&self, path: &str) -> String {
    format!("http://localhost:{}{}", self.port, path)
  }

  /// Unsigned request to a path under `/_apis/artifactcache`.
  pub fn raw(&self, method: Method, path: &str) -> RequestBuilder {
    self
      .client
      .request(method, self.url(&format!("/_apis/artifactcache{path}")))
  }

  /// Request to a path under `/_apis/artifactcache`, signed like the runner does.
  pub fn request(&self, method: Method, path: &str) -> RequestBuilder {
    let timestamp = chrono::Utc::now().timestamp().to_string();
    sign(
      self.raw(method, path),
      SECRET,
      &self.repo,
      "1",
      &timestamp,
      &self.write_isolation_key,
    )
  }

  pub async fn reserve(&self, key: &str, version: &str, size: i64) -> Response {
    self
      .request(Method::POST, "/caches")
      .json(&json!({ "key": key, "version": version, "cacheSize": size }))
      .send()
      .await
      .expect("reserve request")
  }

  /// Reserve and return the cache id.
  pub async fn reserve_id(&self, key: &str, version: &str, size: i64) -> i64 {
    let resp = self.reserve(key, version, size).await;
    assert_eq!(resp.status(), StatusCode::OK, "reserve");
    resp.json::<Value>().await.unwrap()["cacheId"]
      .as_i64()
      .expect("cacheId")
  }

  pub async fn upload_chunk(&self, id: i64, start: usize, data: &[u8]) -> Response {
    let end = start + data.len() - 1;
    self
      .request(Method::PATCH, &format!("/caches/{id}"))
      .header("Content-Range", format!("bytes {start}-{end}/*"))
      .header("Content-Type", "application/octet-stream")
      .body(data.to_vec())
      .send()
      .await
      .expect("upload request")
  }

  pub async fn commit(&self, id: i64) -> Response {
    self
      .request(Method::POST, &format!("/caches/{id}"))
      .send()
      .await
      .expect("commit request")
  }

  /// Reserve, upload `data` in `chunk`-sized parts and commit; returns the id.
  pub async fn store(&self, key: &str, version: &str, data: &[u8], chunk: usize) -> i64 {
    let id = self.reserve_id(key, version, data.len() as i64).await;
    for (i, part) in data.chunks(chunk).enumerate() {
      let resp = self.upload_chunk(id, i * chunk, part).await;
      assert_eq!(resp.status(), StatusCode::OK, "upload chunk {i}");
    }
    assert_eq!(self.commit(id).await.status(), StatusCode::OK, "commit");
    id
  }

  pub async fn find(&self, keys: &[&str], version: &str) -> Response {
    self
      .request(
        Method::GET,
        // ponytail: only escapes '%', enough for the keys used in tests
        &format!(
          "/cache?keys={}&version={version}",
          keys.join(",").replace('%', "%25")
        ),
      )
      .header("Forgejo-Cache-Host", "http://cache.local")
      .header("Forgejo-Cache-RunId", "7")
      .send()
      .await
      .expect("find request")
  }

  /// `find` that must hit; returns the JSON body.
  pub async fn find_hit(&self, keys: &[&str], version: &str) -> Value {
    let resp = self.find(keys, version).await;
    assert_eq!(resp.status(), StatusCode::OK, "find {keys:?}");
    resp.json().await.unwrap()
  }

  /// Download a cache entry, optionally with an HTTP `Range` (e.g. `bytes=0-9`).
  pub async fn download(&self, id: i64, range: Option<&str>) -> Response {
    let mut req = self.request(Method::GET, &format!("/artifacts/{id}"));
    if let Some(range) = range {
      req = req.header("Range", range);
    }
    req.send().await.expect("download request")
  }
}

/// Add the `Forgejo-Cache-*` auth headers, MAC'd with `secret`.
pub fn sign(
  req: RequestBuilder,
  secret: &str,
  repo: &str,
  run: &str,
  timestamp: &str,
  wik: &str,
) -> RequestBuilder {
  let mut mac = hmac::Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
  mac.update(format!("{repo}>{run}>{timestamp}>{wik}").as_bytes());
  req
    .header("Forgejo-Cache-Repo", repo)
    .header("Forgejo-Cache-RunNumber", run)
    .header("Forgejo-Cache-Timestamp", timestamp)
    .header("Forgejo-Cache-WriteIsolationKey", wik)
    .header(
      "Forgejo-Cache-MAC",
      hex::encode(mac.finalize().into_bytes()),
    )
}

/// Deterministic, non-repeating-ish test payload.
pub fn payload(len: usize, seed: u8) -> Vec<u8> {
  (0..len)
    .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
    .collect()
}

/// Configure the env for a throwaway server and start its S3 bucket.
async fn prepare_env() -> TestContainer {
  let container = GenericImage::new("adobe/s3mock", "5.2.3")
    .with_exposed_port(ContainerPort::Tcp(9090))
    .with_wait_for(WaitFor::message_on_stdout("Started S3MockApplication"))
    .with_env_var("COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS", BUCKET)
    .start()
    .await
    .expect("start S3Mock container");
  let host = format!(
    "http://{}:{}",
    container.get_host().await.unwrap(),
    container.get_host_port_ipv4(9090).await.unwrap()
  );

  unsafe {
    std::env::set_var("PORT", "0");
    std::env::set_var("DB_URL", "sqlite::memory:");
    std::env::set_var("CACHE_SECRET", SECRET);
    std::env::set_var("LOG_LEVEL", "off");
    std::env::set_var("S3_BUCKET", BUCKET);
    std::env::set_var("S3_REGION", "us-east-1");
    std::env::set_var("S3_HOST", host);
    std::env::set_var("S3_ACCESS_KEY", "test");
    std::env::set_var("S3_SECRET_KEY", "test");
    std::env::set_var("S3_FORCE_PATH_STYLE", "true");
  }

  TestContainer::S3(container)
}
