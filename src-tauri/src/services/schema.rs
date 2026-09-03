// SOT: schema-service, catalog-loading, column-loading, foreign-key-loading, ddl-loading

use crate::error::AppResult;
use crate::guard::SessionCtx;
use crate::model::{ColumnInfo, ForeignKey, SchemaCatalog, TableRef};

pub async fn catalog(ctx: &SessionCtx) -> AppResult<SchemaCatalog> {
    ctx.integration.catalog().await
}

pub async fn columns(ctx: &SessionCtx, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
    ctx.integration.columns(table).await
}

pub async fn foreign_keys(ctx: &SessionCtx) -> AppResult<Vec<ForeignKey>> {
    ctx.integration.foreign_keys().await
}

pub async fn ddl(ctx: &SessionCtx, table: &TableRef) -> AppResult<Option<String>> {
    ctx.integration.ddl(table).await
}
