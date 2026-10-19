use std::str::FromStr;

use axum::{Extension, Router, extract::FromRequestParts};
use centaurus::{
  backend::request::extract::StateExtractExt,
  bail,
  error::ErrorReport,
  eyre::{Context, ContextCompat},
};
use hmac::{KeyInit, Mac};
use http::request::Parts;
use sea_orm::sqlx::types::chrono::Utc;
use sha2::Sha256;

use crate::config::Config;

pub fn state(router: Router, config: &Config) -> Router {
  router.layer(Extension(AuthSecret(config.cache_secret.clone())))
}

#[derive(Clone)]
struct AuthSecret(String);

pub struct Auth {
  pub repo: String,
  pub write_isolation_key: String,
}

impl<S: Sync> FromRequestParts<S> for Auth {
  type Rejection = ErrorReport;

  async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
    let repo: String = get_header(parts, "Forgejo-Cache-Repo")?;
    let run_number: String = get_header(parts, "Forgejo-Cache-RunNumber")?;
    let timestamp: u64 = get_header(parts, "Forgejo-Cache-Timestamp")?;
    let write_isolation_key: String =
      get_header(parts, "Forgejo-Cache-WriteIsolationKey").unwrap_or_default();
    let mac: String = get_header(parts, "Forgejo-Cache-MAC")?;

    if timestamp > Utc::now().timestamp() as u64 {
      bail!(FORBIDDEN, "Request timestamp is in the future");
    }

    let secret = parts.extract_state::<AuthSecret>().await;

    let mut mac_data = repo.clone();
    mac_data.push('>');
    mac_data.push_str(&run_number);
    mac_data.push('>');
    mac_data.push_str(&timestamp.to_string());
    mac_data.push('>');
    mac_data.push_str(&write_isolation_key);

    let mut expected = hmac::Hmac::<Sha256>::new_from_slice(secret.0.as_bytes())?;
    expected.update(mac_data.as_bytes());
    let expected_mac = hex::encode(expected.finalize().into_bytes());

    if mac != expected_mac {
      bail!(FORBIDDEN, "Invalid MAC");
    }

    Ok(Self {
      repo,
      write_isolation_key,
    })
  }
}

fn get_header<T: FromStr>(parts: &Parts, header_name: &str) -> Result<T, ErrorReport>
where
  <T as FromStr>::Err: Send + Sync + std::error::Error + 'static,
{
  let header_value = parts
    .headers
    .get(header_name)
    .context(format!("Missing required header: {}", header_name))?
    .to_str()
    .context(format!("Invalid header value for: {}", header_name))?
    .parse()
    .context(format!("Failed to parse header value for: {}", header_name))?;
  Ok(header_value)
}

#[cfg(test)]
mod test {
  use axum::{body::Body, routing::get};
  use http::{Request, StatusCode};
  use tower::ServiceExt;

  use super::*;

  const SECRET: &str = "test-secret";

  fn mac(secret: &str, repo: &str, run: &str, timestamp: &str, wik: &str) -> String {
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{repo}>{run}>{timestamp}>{wik}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
  }

  fn now() -> String {
    Utc::now().timestamp().to_string()
  }

  /// Headers of a correctly signed request, as `(name, value)` pairs.
  fn signed(wik: &str) -> Vec<(&'static str, String)> {
    let ts = now();
    vec![
      ("Forgejo-Cache-Repo", "owner/repo".into()),
      ("Forgejo-Cache-RunNumber", "42".into()),
      ("Forgejo-Cache-Timestamp", ts.clone()),
      ("Forgejo-Cache-WriteIsolationKey", wik.into()),
      (
        "Forgejo-Cache-MAC",
        mac(SECRET, "owner/repo", "42", &ts, wik),
      ),
    ]
  }

  fn without(headers: Vec<(&'static str, String)>, name: &str) -> Vec<(&'static str, String)> {
    headers.into_iter().filter(|(n, _)| *n != name).collect()
  }

  fn with(
    headers: Vec<(&'static str, String)>,
    name: &'static str,
    value: String,
  ) -> Vec<(&'static str, String)> {
    let mut headers = without(headers, name);
    headers.push((name, value));
    headers
  }

  async fn extract_with_secret(
    headers: Vec<(&'static str, String)>,
    secret: &str,
  ) -> Result<Auth, ErrorReport> {
    let mut builder = Request::builder();
    for (name, value) in headers {
      builder = builder.header(name, value);
    }
    let (mut parts, _) = builder
      .extension(AuthSecret(secret.into()))
      .body(())
      .unwrap()
      .into_parts();
    Auth::from_request_parts(&mut parts, &()).await
  }

  async fn extract(headers: Vec<(&'static str, String)>) -> Result<Auth, ErrorReport> {
    extract_with_secret(headers, SECRET).await
  }

  fn status(result: Result<Auth, ErrorReport>) -> StatusCode {
    match result {
      Ok(_) => StatusCode::OK,
      Err(e) => e.status,
    }
  }

  #[tokio::test]
  async fn valid_signature_is_accepted() {
    let auth = extract(signed("refs/pull/1")).await.unwrap();
    assert_eq!(auth.repo, "owner/repo");
    assert_eq!(auth.write_isolation_key, "refs/pull/1");
  }

  #[tokio::test]
  async fn missing_required_headers_are_rejected() {
    for name in [
      "Forgejo-Cache-Repo",
      "Forgejo-Cache-RunNumber",
      "Forgejo-Cache-Timestamp",
      "Forgejo-Cache-MAC",
    ] {
      let result = extract(without(signed(""), name)).await;
      assert_eq!(status(result), StatusCode::BAD_REQUEST, "missing {name}");
    }
  }

  #[tokio::test]
  async fn missing_write_isolation_key_defaults_to_empty() {
    let headers = without(signed(""), "Forgejo-Cache-WriteIsolationKey");
    let auth = extract(headers).await.unwrap();
    assert_eq!(auth.write_isolation_key, "");
  }

  #[tokio::test]
  async fn missing_write_isolation_key_does_not_match_non_empty_mac() {
    let headers = without(signed("refs/pull/1"), "Forgejo-Cache-WriteIsolationKey");
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn non_numeric_timestamp_is_rejected() {
    let headers = with(signed(""), "Forgejo-Cache-Timestamp", "soon".into());
    assert_eq!(status(extract(headers).await), StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn negative_timestamp_is_rejected() {
    let headers = with(signed(""), "Forgejo-Cache-Timestamp", "-1".into());
    assert_eq!(status(extract(headers).await), StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn future_timestamp_is_forbidden() {
    let ts = (Utc::now().timestamp() + 3600).to_string();
    let headers = with(
      with(signed(""), "Forgejo-Cache-Timestamp", ts.clone()),
      "Forgejo-Cache-MAC",
      mac(SECRET, "owner/repo", "42", &ts, ""),
    );
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn wrong_mac_is_forbidden() {
    let headers = with(signed(""), "Forgejo-Cache-MAC", "00".repeat(32));
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn mac_over_other_repo_is_forbidden() {
    let headers = with(signed(""), "Forgejo-Cache-Repo", "other/repo".into());
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn mac_over_other_write_isolation_key_is_forbidden() {
    let headers = with(
      signed(""),
      "Forgejo-Cache-WriteIsolationKey",
      "refs/pull/1".into(),
    );
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn uppercase_mac_is_forbidden() {
    let headers = signed("");
    let upper = headers[4].1.to_uppercase();
    let headers = with(headers, "Forgejo-Cache-MAC", upper);
    assert_eq!(status(extract(headers).await), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn wrong_secret_is_forbidden() {
    let result = extract_with_secret(signed(""), "other-secret").await;
    assert_eq!(status(result), StatusCode::FORBIDDEN);
  }

  #[tokio::test]
  async fn non_ascii_header_value_is_rejected() {
    let (mut parts, _) = Request::builder()
      .extension(AuthSecret(SECRET.into()))
      .body(())
      .unwrap()
      .into_parts();
    for (name, value) in signed("") {
      parts.headers.insert(name, value.parse().unwrap());
    }
    parts.headers.insert(
      "Forgejo-Cache-Repo",
      http::HeaderValue::from_bytes("ownér/repo".as_bytes()).unwrap(),
    );
    let result = Auth::from_request_parts(&mut parts, &()).await;
    assert_eq!(status(result), StatusCode::BAD_REQUEST);
  }

  #[tokio::test]
  async fn state_installs_the_secret_for_the_extractor() {
    let config = Config {
      cache_secret: SECRET.into(),
      ..Default::default()
    };
    let router = state(
      Router::new().route("/", get(|auth: Auth| async move { auth.repo })),
      &config,
    );

    let mut req = Request::builder().uri("/");
    for (name, value) in signed("") {
      req = req.header(name, value);
    }
    let resp = router
      .oneshot(req.body(Body::empty()).unwrap())
      .await
      .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
  }
}
