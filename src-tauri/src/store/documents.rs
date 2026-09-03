// SOT: documents-table, dashboards-persistence, workflows-persistence, diagrams-persistence

use crate::error::{AppError, AppResult};
use crate::model::{Document, DocumentBody, DocumentKind};
use crate::store::{now_rfc3339, Store};
use rusqlite::{params, Row};

// WHAT:  One table for every JSON-bodied artefact (dashboard, workflow, diagram).
// WHY:   They share a lifecycle (name, tags, connection scope, timestamps); the
//        typed body is validated by serde on the way in and out.
fn from_row(row: &Row<'_>) -> rusqlite::Result<Document> {
    let kind_raw: String = row.get(1)?;
    let body_raw: String = row.get(4)?;
    let tags: String = row.get(5)?;
    let body: DocumentBody = serde_json::from_str(&body_raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(Document {
        id: row.get(0)?,
        kind: DocumentKind::parse(&kind_raw).unwrap_or(DocumentKind::Dashboard),
        connection_id: row.get(2)?,
        name: row.get(3)?,
        body,
        tags: tags.split(',').map(str::trim).filter(|t| !t.is_empty()).map(String::from).collect(),
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

const COLUMNS: &str = "id, kind, connection_id, name, body, tags, created_at, updated_at";

impl Store {
    pub fn list_documents(&self, kind: DocumentKind) -> AppResult<Vec<Document>> {
        let sql = format!("SELECT {COLUMNS} FROM documents WHERE kind = ?1 ORDER BY lower(name)");
        let mut stmt = self.conn().prepare(&sql).map_err(AppError::store)?;
        let rows = stmt
            .query_map(params![kind.as_str()], from_row)
            .map_err(AppError::store)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(AppError::store)?;
        Ok(rows)
    }

    pub fn get_document(&self, id: &str) -> AppResult<Document> {
        let sql = format!("SELECT {COLUMNS} FROM documents WHERE id = ?1");
        self.conn()
            .query_row(&sql, params![id], from_row)
            .map_err(|_| AppError::not_found(format!("Document {id} does not exist.")))
    }

    pub fn upsert_document(&self, doc: &Document) -> AppResult<Document> {
        let now = now_rfc3339();
        let id = if doc.id.is_empty() { uuid::Uuid::new_v4().to_string() } else { doc.id.clone() };
        let body = serde_json::to_string(&doc.body).map_err(AppError::store)?;
        self.conn()
            .execute(
                "INSERT INTO documents (id, kind, connection_id, name, body, tags, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7) \
                 ON CONFLICT(id) DO UPDATE SET connection_id = excluded.connection_id, name = excluded.name, \
                 body = excluded.body, tags = excluded.tags, updated_at = excluded.updated_at",
                params![id, doc.kind.as_str(), doc.connection_id, doc.name.trim(), body, doc.tags.join(","), now],
            )
            .map_err(AppError::store)?;
        self.get_document(&id)
    }

    pub fn delete_document(&self, id: &str) -> AppResult<()> {
        self.conn().execute("DELETE FROM documents WHERE id = ?1", params![id]).map_err(AppError::store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DashboardBody, WorkflowBody, WorkflowStep};

    #[test]
    fn documents_round_trip_by_kind() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        let dash = store
            .upsert_document(&Document {
                id: String::new(),
                kind: DocumentKind::Dashboard,
                connection_id: None,
                name: "Dashboard 1".into(),
                body: DocumentBody::Dashboard(DashboardBody::default()),
                tags: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
            })
            .unwrap_or_else(|e| panic!("{e}"));
        store
            .upsert_document(&Document {
                id: String::new(),
                kind: DocumentKind::Workflow,
                connection_id: None,
                name: "Workflow 1".into(),
                body: DocumentBody::Workflow(WorkflowBody {
                    steps: vec![WorkflowStep { id: "s1".into(), name: "count".into(), connection_id: None, sql: "select 1".into(), stop_on_error: true }],
                }),
                tags: vec!["ops".into()],
                created_at: String::new(),
                updated_at: String::new(),
            })
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(store.list_documents(DocumentKind::Dashboard).unwrap_or_default().len(), 1);
        let flows = store.list_documents(DocumentKind::Workflow).unwrap_or_default();
        assert!(matches!(flows.first().map(|d| &d.body), Some(DocumentBody::Workflow(b)) if b.steps.len() == 1));
        store.delete_document(&dash.id).unwrap_or_else(|e| panic!("{e}"));
        assert!(store.list_documents(DocumentKind::Dashboard).unwrap_or_default().is_empty());
    }
}
