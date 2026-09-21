use axum::body::Bytes;
use centaurus::{
  error::{ErrorReportStatusExt, Result},
  storage::{FileStorage, MultipartStore, PartId, StoragePath},
};
use http::StatusCode;

pub struct UploadPart {
  pub part_number: i32,
  pub etag: Option<String>,
  pub size: i64,
}

/// S3 numbers parts from 1, the `MultipartStore` API from 0.
fn part_idx(part_number: i32) -> usize {
  part_number as usize - 1
}

pub trait StorageExt {
  fn multipart_store(&self) -> Result<&dyn MultipartStore>;
  async fn create_multipart_upload(&self, key: &StoragePath) -> Result<String>;
  async fn upload_part(
    &self,
    key: &StoragePath,
    upload_id: &str,
    part_number: i32,
    data: Bytes,
  ) -> Result<String>;
  async fn complete_multipart_upload(
    &self,
    key: &StoragePath,
    upload_id: &str,
    parts: Vec<UploadPart>,
  ) -> Result<()>;
  async fn cancel_multipart_upload(&self, key: &StoragePath, upload_id: &str) -> Result<()>;
}

impl StorageExt for FileStorage {
  /// Always present in practice: the config panics unless S3 is configured.
  fn multipart_store(&self) -> Result<&dyn MultipartStore> {
    self.multipart().status_context(
      StatusCode::INTERNAL_SERVER_ERROR,
      "Storage backend has no multipart API",
    )
  }

  async fn create_multipart_upload(&self, key: &StoragePath) -> Result<String> {
    Ok(self.multipart_store()?.create_multipart(key).await?)
  }

  async fn upload_part(
    &self,
    key: &StoragePath,
    upload_id: &str,
    part_number: i32,
    data: Bytes,
  ) -> Result<String> {
    let part = self
      .multipart_store()?
      .put_part(
        key,
        &upload_id.to_string(),
        part_idx(part_number),
        data.into(),
      )
      .await?;

    Ok(part.content_id)
  }

  async fn complete_multipart_upload(
    &self,
    key: &StoragePath,
    upload_id: &str,
    mut parts: Vec<UploadPart>,
  ) -> Result<()> {
    parts.sort_unstable_by_key(|part| part.part_number);

    let parts = parts
      .into_iter()
      .map(|part| {
        Ok(PartId {
          content_id: part.etag.status_context(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Cache upload part was never uploaded",
          )?,
        })
      })
      .collect::<Result<Vec<_>>>()?;

    self
      .multipart_store()?
      .complete_multipart(key, &upload_id.to_string(), parts)
      .await?;

    Ok(())
  }

  async fn cancel_multipart_upload(&self, key: &StoragePath, upload_id: &str) -> Result<()> {
    self
      .multipart_store()?
      .abort_multipart(key, &upload_id.to_string())
      .await?;

    Ok(())
  }
}
