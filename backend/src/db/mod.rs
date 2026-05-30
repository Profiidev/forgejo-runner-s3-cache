use centaurus::db::init::Connection;

mod cache_upload;

pub trait DBTrait {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_>;
}

impl DBTrait for Connection {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_> {
    cache_upload::CacheUploadTable::new(self)
  }
}
