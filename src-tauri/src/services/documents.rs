// SOT: documents-service, dashboards-service, workflows-service, diagrams-service

use crate::error::{AppError, AppResult};
use crate::model::{Document, DocumentBody, DocumentKind};
use crate::state::AppState;

pub fn list(state: &AppState, kind: DocumentKind) -> AppResult<Vec<Document>> {
    state.with_store(|store| store.list_documents(kind))
}

pub fn get(state: &AppState, id: &str) -> AppResult<Document> {
    state.with_store(|store| store.get_document(id))
}

// WHAT:  Saves a document after checking the body matches its declared kind.
pub fn save(state: &AppState, doc: &Document) -> AppResult<Document> {
    if doc.name.trim().is_empty() {
        return Err(AppError::invalid_input("A name is required."));
    }
    let matches = matches!(
        (&doc.kind, &doc.body),
        (DocumentKind::Dashboard, DocumentBody::Dashboard(_))
            | (DocumentKind::Workflow, DocumentBody::Workflow(_))
            | (DocumentKind::Diagram, DocumentBody::Diagram(_))
    );
    if !matches {
        return Err(AppError::invalid_input("Document body does not match its kind."));
    }
    state.with_store(|store| store.upsert_document(doc))
}

pub fn delete(state: &AppState, id: &str) -> AppResult<()> {
    state.with_store(|store| store.delete_document(id))
}
