use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
  async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .create_table(
        Table::create()
          .table(CacheUpload::Table)
          .if_not_exists()
          .col(pk_uuid(CacheUpload::Id))
          .col(string(CacheUpload::Repo))
          .col(string(CacheUpload::Key))
          .col(string(CacheUpload::Version))
          .col(string(CacheUpload::WriteIsolationKey))
          .col(big_integer_null(CacheUpload::Size))
          .col(date_time(CacheUpload::CreatedAt))
          .col(string_null(CacheUpload::S3UploadId))
          .to_owned(),
      )
      .await?;

    manager
      .create_table(
        Table::create()
          .table(CacheUploadPart::Table)
          .if_not_exists()
          .col(pk_uuid(CacheUploadPart::Id))
          .col(uuid(CacheUploadPart::CacheEntryId))
          .col(string(CacheUploadPart::PartInfo))
          .col(big_integer(CacheUploadPart::Size))
          .foreign_key(
            ForeignKey::create()
              .from(CacheUploadPart::Table, CacheUploadPart::CacheEntryId)
              .to(CacheUpload::Table, CacheUpload::Id)
              .on_delete(ForeignKeyAction::Cascade)
              .on_update(ForeignKeyAction::Cascade),
          )
          .to_owned(),
      )
      .await
  }

  async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .drop_table(Table::drop().table(CacheUploadPart::Table).to_owned())
      .await?;

    manager
      .drop_table(Table::drop().table(CacheUpload::Table).to_owned())
      .await
  }
}

#[derive(DeriveIden)]
enum CacheUpload {
  Table,
  Id,
  Repo,
  Key,
  Version,
  WriteIsolationKey,
  Size,
  CreatedAt,
  S3UploadId,
}

#[derive(DeriveIden)]
enum CacheUploadPart {
  Table,
  Id,
  CacheEntryId,
  PartInfo,
  Size,
}
