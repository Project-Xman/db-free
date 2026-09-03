// SOT: query-service, sql-execution

use crate::error::AppResult;
use crate::guard::SessionCtx;
use crate::model::QueryOutcome;

pub async fn execute(ctx: &SessionCtx, sql: &str, max_rows: usize) -> AppResult<QueryOutcome> {
    let statements = ctx.integration.execute(sql, max_rows).await?;
    Ok(QueryOutcome { statements, elapsed_ms: ctx.elapsed_ms() })
}
