// SOT: data-service, table-paging, primary-key-ordering, filtered-count

use crate::error::{AppError, AppResult};
use crate::guard::SessionCtx;
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::qualified_name_for;
use crate::model::{PageQuery, SortRule, TablePage, TableRef};

// WHAT:  One page of a table with sort + filters, ordered deterministically.
// WHY:   Offset paging without a stable order duplicates/skips rows, so the
//        primary key is appended when the caller gave no sort.
// HOW:   Columns come from the catalog (an empty table still shows headers);
//        sort/filter columns are validated against them before any SQL is built.
//        Filters make the count exact; otherwise the cheap estimate is used.
// WHERE: src/features/grid/TableBrowser.tsx (consumer)
pub async fn table_page(ctx: &SessionCtx, table: &TableRef, query: &PageQuery) -> AppResult<TablePage> {
    let columns = ctx.integration.columns(table).await?;
    if columns.is_empty() {
        return Err(AppError::not_found(format!("Table \"{}\" has no columns or does not exist.", table.name)));
    }
    validate_columns(&columns, &query.sort, &query.filters)?;

    let mut sort = query.sort.clone();
    for pk in columns.iter().filter(|c| c.primary_key) {
        if !sort.iter().any(|s| s.column == pk.name) {
            sort.push(SortRule { column: pk.name.clone(), desc: false });
        }
    }
    let effective = PageQuery { sort, filters: query.filters.clone(), offset: query.offset, limit: query.limit };
    let result = ctx.integration.fetch_page(table, &effective).await?;

    let (total, total_exact) = if !query.filters.is_empty() {
        (Some(ctx.integration.count(table, &query.filters).await?), true)
    } else if query.offset == 0 {
        match ctx.integration.row_estimate(table).await.unwrap_or(None) {
            Some(n) => (Some(n), ctx.integration.engine() == crate::model::Engine::Sqlite),
            None => (None, false),
        }
    } else {
        (None, false)
    };

    Ok(TablePage { columns, rows: result.rows, offset: query.offset, total, total_exact })
}

// WHAT:  The SELECT the adapter ran for a page, for the System history log.
pub fn describe_page_sql(ctx: &SessionCtx, table: &TableRef, query: &PageQuery) -> String {
    let engine = ctx.connection.engine;
    format!(
        "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
        qualified_name_for(engine, table),
        where_clause(engine, &query.filters),
        order_clause(engine, &query.sort),
        query.limit,
        query.offset
    )
}
