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
  _unused_entry: Arc<JoinHandle<()>>,
  _incomplete_entry: Arc<JoinHandle<()>>,
  _upload: Arc<JoinHandle<()>>,
}

impl Gc {
  pub fn init(db: Connection, storage: FileStorage) -> Self {
    let unused_entry = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        unused_entry_gc(db, storage).await;
      }
    });

    let incomplete_entry = spawn({
      let db = db.clone();
      async move {
        incomplete_entry_gc(db).await;
      }
    });

    let upload = spawn({
      let db = db.clone();
      let storage = storage.clone();
      async move {
        upload_gc(db, storage).await;
      }
    });

    Gc {
      _unused_entry: Arc::new(unused_entry),
      _incomplete_entry: Arc::new(incomplete_entry),
      _upload: Arc::new(upload),
    }
  }
}

async fn unused_entry_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    info!("Running GC for unused cache entries");
    // cleanup entries that haven't been used for more than 7 days
    let before = Utc::now() - Duration::days(7);
    let Ok(entries) = db
      .cache_entry()
      .find_unused_entries(before.naive_utc())
      .await
      .map_err(|e| {
        warn!("Failed to query unused cache entries for GC: {e}");
      })
    else {
      continue;
    };

    info!("Found {} unused cache entries for GC", entries.len());

    for entry in entries {
      if let Err(e) = storage.delete_file(&entry.id.to_string()).await {
        warn!("Failed to delete unused cache object for GC: {e}");
        continue;
      }

      if let Err(e) = db.cache_entry().delete_by_id(entry.id).await {
        warn!("Failed to delete unused cache entry for GC: {e}");
      }
    }

    sleep(Duration::hours(1).to_std().unwrap()).await;
  }
}

async fn incomplete_entry_gc(db: Connection) -> ! {
  loop {
    info!("Running GC for incomplete cache entries");
    // cleanup incomplete entries that failed to be marked complete for more than 5 minutes
    let before = Utc::now() - Duration::minutes(5);
    if let Err(e) = db
      .cache_entry()
      .clean_incomplete_entries(before.naive_utc())
      .await
    {
      warn!("Failed to delete incomplete cache entries for GC: {e}");
    }

    sleep(Duration::minutes(5).to_std().unwrap()).await;
  }
}

async fn upload_gc(db: Connection, storage: FileStorage) -> ! {
  loop {
    info!("Running GC for incomplete uploads");
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
