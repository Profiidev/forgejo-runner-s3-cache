use aws_sdk_s3::primitives::ByteStream;
use axum::body::Bytes;
use centaurus::{
  bail,
  error::{ErrorReportStatusExt, Result},
  eyre::Context,
  storage::FileStorage,
};
use http::StatusCode;
use tokio::{fs, io};
use tracing::warn;

pub struct UploadPart {
  pub part_number: i32,
  pub etag: Option<String>,
  pub size: i64,
}

pub trait StorageExt {
  async fn create_multipart_upload(&self, key: &str) -> Result<Option<String>>;
  async fn upload_part(
    &self,
    key: &str,
    upload_id: Option<&str>,
    part_number: i32,
    data: Bytes,
  ) -> Result<Option<String>>;
  async fn complete_multipart_upload(
    &self,
    key: &str,
    upload_id: Option<&str>,
    parts: Vec<UploadPart>,
  ) -> Result<()>;
  async fn cancel_multipart_upload(
    &self,
    key: &str,
    upload_id: Option<&str>,
    parts: &[i32],
  ) -> Result<()>;
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

  async fn complete_multipart_upload(
    &self,
    key: &str,
    upload_id: Option<&str>,
    mut parts: Vec<UploadPart>,
  ) -> Result<()> {
    parts.sort_unstable_by_key(|part| part.part_number);

    match (self, upload_id) {
      (FileStorage::Local(path), None) => {
        let mut file = fs::File::create(path.join(key)).await?;

        for part in parts {
          let part_path = path.join(format!("{key}-{}", part.part_number));
          let mut part = fs::File::open(&part_path).await?;
          io::copy(&mut part, &mut file).await?;
          drop(part);
          fs::remove_file(part_path).await?;
        }

        Ok(())
      }
      (FileStorage::S3 { client, bucket }, Some(upload_id)) => {
        let completed_parts = parts
          .into_iter()
          .map(|part| {
            aws_sdk_s3::types::CompletedPart::builder()
              .set_e_tag(part.etag)
              .part_number(part.part_number)
              .build()
          })
          .collect();

        client
          .complete_multipart_upload()
          .bucket(bucket)
          .key(key)
          .upload_id(upload_id)
          .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
              .set_parts(Some(completed_parts))
              .build(),
          )
          .send()
          .await
          .context("Failed to complete multipart upload in S3")?;

        Ok(())
      }
      _ => bail!("Invalid storage configuration for completing multipart upload"),
    }
  }

  async fn cancel_multipart_upload(
    &self,
    key: &str,
    upload_id: Option<&str>,
    parts: &[i32],
  ) -> Result<()> {
    match (self, upload_id) {
      (FileStorage::Local(path), None) => {
        for part_number in parts {
          let part_path = path.join(format!("{key}-{part_number}"));
          if part_path.exists() {
            fs::remove_file(part_path).await?;
          }
        }

        // also remove the final file if it exists, since the upload is cancelled
        let file_path = path.join(key);
        if file_path.exists() {
          fs::remove_file(file_path).await?;
        }

        Ok(())
      }
      (FileStorage::S3 { client, bucket }, Some(upload_id)) => {
        let _ = client
          .abort_multipart_upload()
          .bucket(bucket)
          .key(key)
          .upload_id(upload_id)
          .send()
          .await
          .context("Failed to abort multipart upload in S3");

        Ok(())
      }
      _ => {
        warn!("Invalid storage configuration for cancelling multipart upload");
        Ok(())
      }
    }
  }
}
