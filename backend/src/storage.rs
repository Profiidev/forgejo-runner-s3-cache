use centaurus::{
  error::{ErrorReportStatusExt, Result},
  eyre::Context,
  storage::FileStorage,
};
use http::StatusCode;

pub trait StorageExt {
  async fn create_multipart_upload(&self, key: &str) -> Result<Option<String>>;
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
}
