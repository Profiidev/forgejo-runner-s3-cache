pub use sea_orm_migration::prelude::*;

pub struct Migrator;

mod m20260530_133703_cache_entry;
mod m20260530_143032_cache_upload;
mod m20260531_140045_cache_cleanup;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
  fn migrations() -> Vec<Box<dyn MigrationTrait>> {
    vec![
            Box::new(m20260530_133703_cache_entry::Migration),
            Box::new(m20260530_143032_cache_upload::Migration),
            Box::new(m20260531_140045_cache_cleanup::Migration),
        ]
  }
}
