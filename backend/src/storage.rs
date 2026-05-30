use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use centaurus::{
  bail,
  error::{ErrorReportStatusExt, Result},
  eyre::Context,
  storage::FileStorage,
};
use http::StatusCode;
use tokio::fs;

pub trait StorageExt {
  async fn create_multipart_upload(&self, key: &str) -> Result<Option<String>>;
  async fn upload_part(
    &self,
    key: &str,
    upload_id: Option<&str>,
    part_number: i32,
    data: Bytes,
  ) -> Result<Option<String>>;
}

impl StorageExt for FileStorage {
  async fn create_multipart_upload(&self, key: &str) -> Result<Option<String>> {
    match self {
      FileStorage::Local(_) => Ok(None),
      FileStorage::S3 { client, bucket } => {
        let multipart_upload = client
          .create_multipart_upload()
          .bucket(bucket)
          .key(key)
          .send()
          .await
          .context("Faield to create multipart upload for file in S3 Bucket")?;

        let upload_id = multipart_upload.upload_id.status_context(
          StatusCode::INTERNAL_SERVER_ERROR,
          "Failed to get upload ID from S3 multipart upload response",
        )?;

        Ok(Some(upload_id))
      }
    }
  }

  async fn upload_part(
    &self,
    key: &str,
    upload_id: Option<&str>,
    part_number: i32,
    data: Bytes,
  ) -> Result<Option<String>> {
    match (self, upload_id) {
      (FileStorage::Local(path), None) => {
        let file_path = path.join(format!("{key}-{part_number}"));
        fs::write(&file_path, &data).await?;

        Ok(None)
      }
      (FileStorage::S3 { client, bucket }, Some(upload_id)) => {
        let part = client
          .upload_part()
          .bucket(bucket)
          .key(key)
          .upload_id(upload_id)
          .part_number(part_number)
          .body(ByteStream::from(data))
          .send()
          .await
          .context("Failed to upload part to S3")?;

        let etag = part.e_tag.status_context(
          StatusCode::INTERNAL_SERVER_ERROR,
          "Failed to get ETag from S3 upload part response",
        )?;

        Ok(Some(etag))
      }
      _ => bail!("Invalid storage configuration for multipart upload"),
    }
  }
}
