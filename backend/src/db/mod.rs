use centaurus::db::init::Connection;

mod cache_entry;
mod cache_upload;

pub trait DBTrait {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_>;
  fn cache_entry(&self) -> cache_entry::CacheEntryTable<'_>;
}

impl DBTrait for Connection {
  fn cache_upload(&self) -> cache_upload::CacheUploadTable<'_> {
    cache_upload::CacheUploadTable::new(self)
  }

  fn cache_entry(&self) -> cache_entry::CacheEntryTable<'_> {
    cache_entry::CacheEntryTable::new(self)
  }
}
