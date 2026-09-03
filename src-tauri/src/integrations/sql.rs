// SOT: sql-clause-builder, where-clause, order-clause, quote-literal, filter-sql

use crate::error::{AppError, AppResult};
use crate::integrations::quote_ident_for;
use crate::model::{ColumnInfo, Engine, FilterOp, FilterRule, SortRule};

// WHAT:  Builds WHERE / ORDER BY fragments for the table browser.
// WHY:   Both SQL engines share the syntax; one builder means one set of tests.
// HOW:   Identifiers are double-quoted and checked against the table's columns;
//        values are single-quoted with '' escaping so user text can never
//        terminate the literal. Comparisons cast to TEXT only for the LIKE family.
// WHERE: src-tauri/src/services/data.rs (caller), src-tauri/src/integrations/{postgres,sqlite}.rs
pub fn quote_literal(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "''"))
}

pub fn validate_columns(columns: &[ColumnInfo], sort: &[SortRule], filters: &[FilterRule]) -> AppResult<()> {
    let known = |name: &str| columns.iter().any(|c| c.name == name);
    for rule in sort {
        if !known(&rule.column) {
            return Err(AppError::invalid_input(format!("Unknown sort column \"{}\".", rule.column)));
        }
    }
    for rule in filters {
        if !known(&rule.column) {
            return Err(AppError::invalid_input(format!("Unknown filter column \"{}\".", rule.column)));
        }
        if rule.op.needs_value() && rule.value.trim().is_empty() {
            return Err(AppError::invalid_input(format!("Filter on \"{}\" needs a value.", rule.column)));
        }
    }
    Ok(())
}

pub fn where_clause(engine: Engine, filters: &[FilterRule]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters.iter().map(|f| predicate(engine, f)).collect();
    format!(" WHERE {}", parts.join(" AND "))
}

pub fn order_clause(engine: Engine, sort: &[SortRule]) -> String {
    if sort.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sort
        .iter()
        .map(|s| format!("{}{}", quote_ident_for(engine, &s.column), if s.desc { " DESC" } else { " ASC" }))
        .collect();
    format!(" ORDER BY {}", parts.join(", "))
}

fn predicate(engine: Engine, rule: &FilterRule) -> String {
    let col = quote_ident_for(engine, &rule.column);
    // MySQL has no TEXT cast target; CHAR is the portable spelling there.
    let text_type = match engine {
        Engine::Mysql | Engine::Mariadb | Engine::Planetscale => "CHAR",
        Engine::Mssql => "NVARCHAR(MAX)",
        Engine::Postgres
        | Engine::Sqlite
        | Engine::Clickhouse
        | Engine::Redis
        | Engine::Mongodb
        | Engine::Libsql
        | Engine::ValTown
        | Engine::CloudflareD1
        | Engine::Supabase
        | Engine::Neon => "TEXT",
    };
    let text_col = format!("CAST({col} AS {text_type})");
    // Postgres LIKE is case-sensitive; every other SQL engine here is case-insensitive by default.
    let like = match engine {
        Engine::Postgres | Engine::Supabase | Engine::Neon => "ILIKE",
        Engine::Mysql
        | Engine::Mariadb
        | Engine::Sqlite
        | Engine::Clickhouse
        | Engine::Redis
        | Engine::Mongodb
        | Engine::Mssql
        | Engine::Libsql
        | Engine::ValTown
        | Engine::CloudflareD1
        | Engine::Planetscale => "LIKE",
    };
    let value = rule.value.trim();
    match rule.op {
        FilterOp::Eq => format!("{col} = {}", quote_literal(value)),
        FilterOp::Ne => format!("{col} <> {}", quote_literal(value)),
        FilterOp::Gt => format!("{col} > {}", quote_literal(value)),
        FilterOp::Gte => format!("{col} >= {}", quote_literal(value)),
        FilterOp::Lt => format!("{col} < {}", quote_literal(value)),
        FilterOp::Lte => format!("{col} <= {}", quote_literal(value)),
        FilterOp::Contains => format!("{text_col} {like} {}", quote_literal(&format!("%{value}%"))),
        FilterOp::StartsWith => format!("{text_col} {like} {}", quote_literal(&format!("{value}%"))),
        FilterOp::EndsWith => format!("{text_col} {like} {}", quote_literal(&format!("%{value}"))),
        FilterOp::In => {
            let items: Vec<String> = value
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(quote_literal)
                .collect();
            if items.is_empty() {
                "FALSE".to_string()
            } else {
                format!("{col} IN ({})", items.join(", "))
            }
        }
        FilterOp::IsNull => format!("{col} IS NULL"),
        FilterOp::IsNotNull => format!("{col} IS NOT NULL"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn literals_cannot_escape() {
        assert_eq!(quote_literal("it's"), "'it''s'");
        let w = where_clause(Engine::Sqlite, &[rule("name", FilterOp::Eq, "x'; DROP TABLE t; --")]);
        assert_eq!(w, " WHERE \"name\" = 'x''; DROP TABLE t; --'");
    }

    #[test]
    fn operators_render() {
        let w = where_clause(
            Engine::Postgres,
            &[
                rule("id", FilterOp::Gte, "5"),
                rule("email", FilterOp::Contains, "@x"),
                rule("tier", FilterOp::In, "gold, basic"),
                rule("note", FilterOp::IsNull, ""),
            ],
        );
        assert_eq!(
            w,
            " WHERE \"id\" >= '5' AND CAST(\"email\" AS TEXT) ILIKE '%@x%' AND \"tier\" IN ('gold', 'basic') AND \"note\" IS NULL"
        );
        assert_eq!(order_clause(Engine::Postgres, &[SortRule { column: "a".into(), desc: true }, SortRule { column: "b".into(), desc: false }]), " ORDER BY \"a\" DESC, \"b\" ASC");
        assert_eq!(order_clause(Engine::Mysql, &[SortRule { column: "a".into(), desc: false }]), " ORDER BY `a` ASC");
        assert_eq!(where_clause(Engine::Mysql, &[rule("name", FilterOp::Contains, "x")]), " WHERE CAST(`name` AS CHAR) LIKE '%x%'");
        assert_eq!(where_clause(Engine::Sqlite, &[]), "");
    }

    #[test]
    fn unknown_columns_rejected() {
        let cols = vec![ColumnInfo { name: "id".into(), data_type: "int".into(), nullable: false, primary_key: true, ordinal: 1 }];
        assert!(validate_columns(&cols, &[SortRule { column: "nope".into(), desc: false }], &[]).is_err());
        assert!(validate_columns(&cols, &[], &[rule("id", FilterOp::Eq, "  ")]).is_err());
        assert!(validate_columns(&cols, &[], &[rule("id", FilterOp::IsNull, "")]).is_ok());
    }
}
