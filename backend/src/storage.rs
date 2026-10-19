use axum::body::Bytes;
use centaurus::{
  error::{ErrorReportStatusExt, Result},
  storage::{FileStorage, MultipartStore, PartId, StoragePath},
};
use http::StatusCode;
use uuid::Uuid;

pub struct UploadPart {
  pub id: i32,
  pub part_number: i32,
  pub start_byte: i64,
  pub etag: Option<String>,
  pub size: i64,
}

/// Where a tail chunk that arrived before any regular chunk is parked until
/// commit, when its part number can be derived.
pub fn tail_path(file_id: Uuid) -> StoragePath {
  StoragePath::from(format!("{file_id}.tail"))
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

#[cfg(test)]
pub mod test {
  use centaurus::storage::{FileStorage, StorageConfig};
  use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
  };

  pub const BUCKET: &str = "cache";

  /// Local-filesystem storage (no multipart API).
  pub async fn init_fs_storage() -> FileStorage {
    let dir = std::env::temp_dir().join(format!("cache-test-storage-{}", uuid::Uuid::new_v4()));
    FileStorage::init(&StorageConfig {
      storage_path: dir.to_string_lossy().into_owned(),
      ..Default::default()
    })
    .await
    .expect("Failed to init FileStorage")
  }

  /// Throwaway S3 compatible server (Adobe S3Mock; the MinIO images are no
  /// longer published) with an empty bucket. Keep the container alive for the
  /// whole test.
  pub async fn init_s3_storage() -> (FileStorage, ContainerAsync<GenericImage>) {
    let container = GenericImage::new("adobe/s3mock", "5.2.3")
      .with_exposed_port(ContainerPort::Tcp(9090))
      .with_wait_for(WaitFor::message_on_stdout("Started S3MockApplication"))
      .with_env_var("COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS", BUCKET)
      // Many JVM containers start in parallel under nextest.
      .with_startup_timeout(std::time::Duration::from_secs(300))
      .start()
      .await
      .expect("Failed to start S3Mock container");

    let config = StorageConfig {
      storage_path: String::new(),
      s3_bucket: Some(BUCKET.into()),
      s3_region: Some("us-east-1".into()),
      s3_host: Some(format!(
        "http://{}:{}",
        container.get_host().await.unwrap(),
        container.get_host_port_ipv4(9090).await.unwrap()
      )),
      s3_access_key: Some("test".into()),
      s3_secret_key: Some("test".into()),
      s3_force_path_style: true,
    };

    let storage = FileStorage::init(&config)
      .await
      .expect("Failed to init S3 FileStorage");
    (storage, container)
  }

  // ---- tests ----

  use axum::body::Bytes;
  use centaurus::storage::StoragePath;
  use http::StatusCode;

  use super::{StorageExt, UploadPart, part_idx};

  fn path() -> StoragePath {
    StoragePath::from(uuid::Uuid::new_v4().to_string())
  }

  async fn read(storage: &FileStorage, path: &StoragePath) -> Vec<u8> {
    let body = storage.get_file(path, None).await.unwrap();
    axum::body::to_bytes(body, usize::MAX)
      .await
      .unwrap()
      .to_vec()
  }

  /// S3 rejects non-final multipart parts smaller than 5 MiB.
  pub const MIN_PART: usize = 5 * 1024 * 1024;

  /// A `MIN_PART`-sized chunk filled with `byte`.
  pub fn big_chunk(byte: u8) -> Bytes {
    Bytes::from(vec![byte; MIN_PART])
  }

  fn part(part_number: i32, etag: &str, size: i64) -> UploadPart {
    UploadPart {
      id: 0,
      part_number,
      start_byte: 0,
      etag: Some(etag.into()),
      size,
    }
  }

  // ---- part_idx ----

  #[test]
  fn part_idx_maps_one_based_part_numbers_to_zero_based_indices() {
    assert_eq!(part_idx(1), 0);
    assert_eq!(part_idx(2), 1);
    assert_eq!(part_idx(10_000), 9_999);
  }

  // ---- multipart on s3 ----

  #[tokio::test]
  async fn multipart_upload_round_trips_concatenated_parts() {
    let (storage, _container) = init_s3_storage().await;
    let key = path();

    let upload_id = storage.create_multipart_upload(&key).await.unwrap();
    let first = storage
      .upload_part(&key, &upload_id, 1, big_chunk(b'a'))
      .await
      .unwrap();
    let second = storage
      .upload_part(&key, &upload_id, 2, Bytes::from_static(b"tail"))
      .await
      .unwrap();
    storage
      .complete_multipart_upload(
        &key,
        &upload_id,
        vec![part(1, &first, MIN_PART as i64), part(2, &second, 4)],
      )
      .await
      .unwrap();

    let mut expected = big_chunk(b'a').to_vec();
    expected.extend_from_slice(b"tail");
    assert_eq!(read(&storage, &key).await, expected);
  }

  #[tokio::test]
  async fn complete_multipart_upload_sorts_parts_given_out_of_order() {
    let (storage, _container) = init_s3_storage().await;
    let key = path();

    let upload_id = storage.create_multipart_upload(&key).await.unwrap();
    let size = MIN_PART as i64;
    let first = storage
      .upload_part(&key, &upload_id, 1, big_chunk(b'a'))
      .await
      .unwrap();
    let second = storage
      .upload_part(&key, &upload_id, 2, big_chunk(b'b'))
      .await
      .unwrap();
    let third = storage
      .upload_part(&key, &upload_id, 3, Bytes::from_static(b"c"))
      .await
      .unwrap();
    storage
      .complete_multipart_upload(
        &key,
        &upload_id,
        vec![
          part(3, &third, 1),
          part(1, &first, size),
          part(2, &second, size),
        ],
      )
      .await
      .unwrap();

    let mut expected = big_chunk(b'a').to_vec();
    expected.extend_from_slice(&big_chunk(b'b'));
    expected.push(b'c');
    assert_eq!(read(&storage, &key).await, expected);
  }

  #[tokio::test]
  async fn complete_multipart_upload_rejects_part_without_etag() {
    let storage = init_fs_storage().await;

    let err = storage
      .complete_multipart_upload(
        &path(),
        "upload",
        vec![
          part(1, "etag", 3),
          UploadPart {
            id: 0,
            part_number: 2,
            start_byte: 0,
            etag: None,
            size: 3,
          },
        ],
      )
      .await
      .unwrap_err();

    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(err.to_string().contains("never uploaded"));
  }

  #[tokio::test]
  async fn cancel_multipart_upload_aborts_the_upload() {
    let (storage, _container) = init_s3_storage().await;
    let key = path();

    let upload_id = storage.create_multipart_upload(&key).await.unwrap();
    let etag = storage
      .upload_part(&key, &upload_id, 1, Bytes::from_static(b"data"))
      .await
      .unwrap();
    storage
      .cancel_multipart_upload(&key, &upload_id)
      .await
      .unwrap();

    assert!(
      storage
        .complete_multipart_upload(&key, &upload_id, vec![part(1, &etag, 4)])
        .await
        .is_err()
    );
    assert!(!storage.exists(&key).await.unwrap());
  }

  // ---- multipart on local fs ----

  #[tokio::test]
  async fn fs_storage_has_no_multipart_store() {
    let storage = init_fs_storage().await;
    let key = path();

    let err = storage.multipart_store().err().unwrap();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

    let err = storage.create_multipart_upload(&key).await.unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

    let err = storage
      .upload_part(&key, "upload", 1, Bytes::from_static(b"data"))
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

    let err = storage
      .complete_multipart_upload(&key, "upload", vec![part(1, "etag", 4)])
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

    let err = storage
      .cancel_multipart_upload(&key, "upload")
      .await
      .unwrap_err();
    assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
  }
}
