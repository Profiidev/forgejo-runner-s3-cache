use std::sync::Arc;

use axum::{Extension, Router};
use centaurus::{db::init::Connection, storage::FileStorage};
use chrono::{Duration, Utc};
use tokio::{spawn, task::JoinHandle, time::sleep};
use tracing::{info, warn};

use crate::{db::DBTrait, storage::StorageExt};

pub fn state(router: Router, db: Connection, storage: FileStorage) -> Router {
  router.layer(Extension(Gc::init(db, storage)))
}

#[derive(Clone)]
struct Gc {
  _upload: Arc<JoinHandle<()>>,
}

impl Gc {
  pub fn init(db: Connection, storage: FileStorage) -> Self {
    let upload = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        upload_gc(db, storage).await;
      }
    });

    Gc {
      _upload: Arc::new(upload),
    }
  }
}

async fn upload_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    // cleanup uploads that didn't receive a new part for more than 5 minutes
    let before = Utc::now() - Duration::minutes(5);
    let Ok(uploads) = db
      .cache_upload()
      .find_incomplete_uploads(before.naive_utc())
      .await
      .map_err(|e| {
        warn!("Failed to query incomplete uploads for GC: {e}");
      })
    else {
      continue;
    };

    info!("Found {} incomplete uploads for GC", uploads.len());

    for (upload, parts) in uploads {
      if storage
        .cancel_multipart_upload(&upload.key, upload.s3_upload_id.as_deref(), &parts)
        .await
        .map_err(|e| {
          warn!("Failed to cancel multipart upload for GC: {e}");
        })
        .is_err()
      {
        continue;
      }

      if let Err(e) = db.cache_upload().delete(upload.id).await {
        warn!("Failed to delete cache upload entry for GC: {e}");
      }
    }

    sleep(Duration::minutes(5).to_std().unwrap()).await;
  }
}
