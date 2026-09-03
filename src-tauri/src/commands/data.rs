// SOT: data-commands, ipc-table-page

use crate::error::AppResult;
use crate::guard;
use crate::model::{PageQuery, TablePage, TableRef};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct TablePageRequest {
    pub connection_id: String,
    pub table: TableRef,
    pub query: PageQuery,
}

#[tauri::command]
pub async fn fetch_table_page(state: State<'_, AppState>, req: TablePageRequest) -> AppResult<TablePage> {
    let query = PageQuery { limit: guard::clamp_page_limit(req.query.limit), ..req.query };
    let table = req.table.clone();
    let query_for_log = query.clone();
    let started = std::time::Instant::now();
    let result = guard::session(&state, &req.connection_id, |ctx| async move {
        let sql = services::data::describe_page_sql(&ctx, &table, &query_for_log);
        let page = services::data::table_page(&ctx, &table, &query).await;
        Ok((sql, page))
    })
    .await;
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match result {
        Ok((sql, Ok(page))) => {
            services::history::record_system(&state, &req.connection_id, &sql, Ok(page.rows.len() as u64), elapsed);
            Ok(page)
        }
        Ok((sql, Err(err))) => {
            services::history::record_system(&state, &req.connection_id, &sql, Err(err.message()), elapsed);
            Err(err)
        }
        Err(err) => Err(err),
    }
}
