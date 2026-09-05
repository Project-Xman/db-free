// SOT: objects-service, object-listing, object-detail-loading, server-stats-loading, vector-search-service, search-service, range-query-service, ledger-history-service

use crate::error::{AppError, AppResult};
use crate::guard::SessionCtx;
use crate::model::{
    ColumnInfo, ObjectDetail, ObjectKind, ObjectProperty, ObjectRef, ObjectSummary, RangeQueryRequest, RangeResult,
    ResultSet, SearchRequest, SearchResult, ServerStats, TableRef, VectorSearchRequest,
};

// WHAT:  Object explorer, administration and playground tools: one call each,
//        straight to the adapter. Every request already passed guard::session.
// WHY:   Adapters answer only for kinds / tools in their `profile()`; the UI
//        reads that profile from SessionInfo, so an unsupported ask is a UI bug,
//        not a runtime branch here.
// WHERE: src-tauri/src/integrations/mod.rs (Integration trait defaults)
pub async fn list(ctx: &SessionCtx, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
    // Fields are answered here rather than by each adapter: `columns` is a
    // required trait method, so every family already knows how to describe its
    // tables, collections, classes or labels. One implementation means one
    // behaviour on all 45 families instead of 45 chances to disagree.
    if kind == ObjectKind::Field {
        let Some(owner) = parent else { return Ok(Vec::new()) };
        return Ok(field_summaries(&ctx.integration.columns(&table_of(ctx, owner)).await?, owner));
    }
    ctx.integration.objects(kind, parent).await
}

pub async fn detail(ctx: &SessionCtx, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    if reference.kind == ObjectKind::Field {
        return field_detail(ctx, reference).await;
    }
    let mut detail = ctx.integration.object_detail(reference).await?;
    // A table's fields are browsable from the table itself. Adapters that
    // already list their own field children keep theirs; the rest get the ones
    // `columns` reports, which is every family with something table-shaped.
    if OWNS_FIELDS.contains(&reference.kind) && !detail.children.iter().any(|c| c.reference.kind == ObjectKind::Field) {
        let owner = qualified(reference);
        if let Ok(columns) = ctx.integration.columns(&ctx.integration.object_table(reference)).await {
            detail.children.extend(field_summaries(&columns, &owner));
            if detail.columns.is_empty() {
                detail.columns = columns;
            }
        }
    }
    Ok(detail)
}

/// Kinds whose members are rows with a describable shape (mirrors the list the
/// profile wrapper uses to advertise `ObjectKind::Field`).
const OWNS_FIELDS: &[ObjectKind] = &[
    ObjectKind::Table,
    ObjectKind::View,
    ObjectKind::MaterializedView,
    ObjectKind::ForeignTable,
    ObjectKind::VirtualTable,
    ObjectKind::Collection,
    ObjectKind::EdgeCollection,
    ObjectKind::Class,
    ObjectKind::Label,
    ObjectKind::Measurement,
];

/// "schema.table" for an object that lives in a namespace, else just its name.
fn qualified(reference: &ObjectRef) -> String {
    match reference.parent.as_deref() {
        Some(parent) if !parent.is_empty() => format!("{parent}.{}", reference.name),
        _ => reference.name.clone(),
    }
}

// WHAT:  The owner key a Field carries ("schema.table", or a bare name) back
//        into the TableRef its family understands.
// HOW:   Split on the first dot — a name may contain dots, a namespace may not —
//        then hand the pieces to the adapter's `object_table`, because `schema`
//        is a namespace in most families and a discriminator in a few.
fn table_of(ctx: &SessionCtx, owner: &str) -> TableRef {
    let (parent, name) = match owner.split_once('.') {
        Some((parent, name)) => (Some(parent.to_string()), name.to_string()),
        None => (None, owner.to_string()),
    };
    ctx.integration.object_table(&ObjectRef { kind: ObjectKind::Table, name, parent })
}

fn field_summaries(columns: &[ColumnInfo], owner: &str) -> Vec<ObjectSummary> {
    columns
        .iter()
        .map(|c| {
            let summary = ObjectSummary::new(ObjectKind::Field, c.name.clone(), Some(owner.to_string()))
                .with_detail(c.data_type.clone());
            if c.primary_key {
                summary.with_badge("PK")
            } else if !c.nullable {
                summary.with_badge("NOT NULL")
            } else {
                summary
            }
        })
        .collect()
}

// WHAT:  One field's properties, plus the foreign key it takes part in when the
//        engine reports one.
async fn field_detail(ctx: &SessionCtx, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let Some(owner) = reference.parent.as_deref() else {
        return Ok(ObjectDetail::empty(reference));
    };
    let table = table_of(ctx, owner);
    let columns = ctx.integration.columns(&table).await?;
    let Some(column) = columns.iter().find(|c| c.name == reference.name) else {
        return Err(AppError::not_found(format!("{} has no field {}", owner, reference.name)));
    };

    let mut properties = vec![
        ObjectProperty { name: "Type".into(), value: column.data_type.clone() },
        ObjectProperty { name: "Nullable".into(), value: yes_no(column.nullable) },
        ObjectProperty { name: "Primary key".into(), value: yes_no(column.primary_key) },
        ObjectProperty { name: "Position".into(), value: column.ordinal.to_string() },
        ObjectProperty { name: "Owner".into(), value: owner.to_string() },
    ];
    // Foreign keys are optional: an engine that cannot report them simply adds
    // nothing here rather than failing the whole detail.
    if let Ok(keys) = ctx.integration.foreign_keys().await {
        for key in keys.iter().filter(|k| k.from_table == table.name && k.from_schema == table.schema && k.from_columns.contains(&column.name)) {
            properties.push(ObjectProperty {
                name: "References".into(),
                value: format!("{}{} ({})", key.to_schema.as_ref().map(|s| format!("{s}.")).unwrap_or_default(), key.to_table, key.to_columns.join(", ")),
            });
        }
    }

    let mut detail = ObjectDetail::empty(reference);
    detail.definition = Some(format!("{} {}{}", column.name, column.data_type, if column.nullable { "" } else { " NOT NULL" }));
    detail.properties = properties;
    detail.columns = vec![column.clone()];
    Ok(detail)
}

fn yes_no(value: bool) -> String {
    if value { "yes".into() } else { "no".into() }
}

pub async fn stats(ctx: &SessionCtx) -> AppResult<ServerStats> {
    ctx.integration.server_stats().await
}

pub async fn vector_search(ctx: &SessionCtx, req: &VectorSearchRequest) -> AppResult<ResultSet> {
    ctx.integration.vector_search(req).await
}

pub async fn search(ctx: &SessionCtx, req: &SearchRequest) -> AppResult<SearchResult> {
    ctx.integration.search(req).await
}

pub async fn query_range(ctx: &SessionCtx, req: &RangeQueryRequest) -> AppResult<RangeResult> {
    ctx.integration.query_range(req).await
}

pub async fn history(ctx: &SessionCtx, reference: &ObjectRef) -> AppResult<ResultSet> {
    ctx.integration.history(reference).await
}
