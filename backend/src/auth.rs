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
  pub run_number: String,
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
      run_number,
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
