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
          .col(pk_uuid(CacheEntry::Id))
          .col(string(CacheEntry::Repo))
          .col(string(CacheEntry::Key))
          .col(string(CacheEntry::Version))
          .col(string(CacheEntry::WriteIsolationKey))
          .col(boolean(CacheEntry::Complete))
          .col(big_integer(CacheEntry::Size))
          .col(date_time(CacheEntry::CreatedAt))
          .col(date_time_null(CacheEntry::UsedAt))
          .col(string(CacheEntry::S3UploadId))
          .to_owned(),
      )
      .await?;

    manager
      .create_table(
        Table::create()
          .table(CachePart::Table)
          .if_not_exists()
          .col(pk_uuid(CachePart::Id))
          .col(uuid(CachePart::CacheEntryId))
          .col(integer(CachePart::PartNumber))
          .col(string(CachePart::ETag))
          .col(big_integer(CachePart::Size))
          .foreign_key(
            ForeignKey::create()
              .from(CachePart::Table, CachePart::CacheEntryId)
              .to(CacheEntry::Table, CacheEntry::Id)
              .on_delete(ForeignKeyAction::Cascade)
              .on_update(ForeignKeyAction::Cascade),
          )
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
  Complete,
  Size,
  CreatedAt,
  UsedAt,
  S3UploadId,
}

#[derive(DeriveIden)]
enum CachePart {
  Table,
  Id,
  CacheEntryId,
  PartNumber,
  ETag,
  Size,
}
