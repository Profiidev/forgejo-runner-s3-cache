use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
  async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .create_table(
        Table::create()
          .table(CacheCleanup::Table)
          .if_not_exists()
          .col(pk_auto(CacheCleanup::Id))
          .col(uuid(CacheCleanup::FileId))
          .to_owned(),
      )
      .await
  }

  async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
    manager
      .drop_table(Table::drop().table(CacheCleanup::Table).to_owned())
      .await
  }
}

#[derive(DeriveIden)]
enum CacheCleanup {
  Table,
  Id,
  FileId,
}
