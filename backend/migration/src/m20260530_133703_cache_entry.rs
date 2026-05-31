use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
  async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .create_table(
        Table::create()
          .table(CacheEntry::Table)
          .if_not_exists()
          .col(pk_auto(CacheEntry::Id))
          .col(string(CacheEntry::Repo))
          .col(string(CacheEntry::Key))
          .col(string(CacheEntry::Version))
          .col(string(CacheEntry::WriteIsolationKey))
          .col(big_integer(CacheEntry::Size))
          .col(date_time(CacheEntry::CreatedAt))
          .col(date_time(CacheEntry::UsedAt))
          .col(boolean(CacheEntry::Complete))
          .col(uuid(CacheEntry::FileId))
          .to_owned(),
      )
      .await?;

    manager
      .create_index(
        Index::create()
          .if_not_exists()
          .name("idx_cache_lookup")
          .table(CacheEntry::Table)
          .col(CacheEntry::Repo)
          .col(CacheEntry::Version)
          .col(CacheEntry::WriteIsolationKey)
          .col(CacheEntry::Complete)
          .col(CacheEntry::Key)
          .unique()
          .to_owned(),
      )
      .await
  }

  async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .drop_table(Table::drop().table(CacheEntry::Table).to_owned())
      .await
  }
}

#[derive(DeriveIden)]
enum CacheEntry {
  Table,
  Id,
  Repo,
  Key,
  Version,
  WriteIsolationKey,
  Size,
  CreatedAt,
  UsedAt,
  Complete,
  FileId,
}
