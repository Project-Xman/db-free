// SOT: statement-classifier, destructive-detection, sql-splitter, read-write-intent

// WHAT:  Splits a script into statements and labels each Read / Write / Destructive.
// WHY:   The block needs intent without executing: read-only locks reject Write,
//        and Destructive statements need explicit confirmation from the user.
// HOW:   A small tokenizer skips strings, quoted identifiers, comments and
//        dollar-quotes, so a `;` or `WHERE` inside a literal never fools it.
//        Unknown leading keywords classify as Write — fail closed.
// WHERE: src-tauri/src/guard/mod.rs (consumer)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Read,
    Write,
    Destructive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedStatement {
    pub text: String,
    pub kind: StatementKind,
    pub reason: Option<String>,
}

pub fn classify(sql: &str) -> Vec<ClassifiedStatement> {
    split_statements(sql)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .map(|text| {
            let words = top_level_words(&text);
            let (kind, reason) = classify_words(&words);
            ClassifiedStatement { text: text.trim().to_string(), kind, reason }
        })
        .collect()
}

pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let len = chars.len();
    while i < len {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            '\'' | '"' | '`' => {
                let quote = c;
                current.push(c);
                i += 1;
                while i < len {
                    current.push(chars[i]);
                    if chars[i] == quote {
                        if chars.get(i + 1).copied() == Some(quote) {
                            current.push(quote);
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            '-' if next == Some('-') => {
                while i < len && chars[i] != '\n' {
                    current.push(chars[i]);
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                current.push_str("/*");
                i += 2;
                while i < len {
                    if chars[i] == '*' && chars.get(i + 1).copied() == Some('/') {
                        current.push_str("*/");
                        i += 2;
                        break;
                    }
                    current.push(chars[i]);
                    i += 1;
                }
            }
            '$' => {
                // Postgres dollar quoting: $$...$$ or $tag$...$tag$
                let mut j = i + 1;
                while j < len && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                if chars.get(j).copied() == Some('$') {
                    let tag: String = chars[i..=j].iter().collect();
                    let tag_len = tag.chars().count();
                    current.push_str(&tag);
                    i = j + 1;
                    while i < len {
                        let window: String = chars[i..len.min(i + tag_len)].iter().collect();
                        if window == tag {
                            current.push_str(&tag);
                            i += tag_len;
                            break;
                        }
                        current.push(chars[i]);
                        i += 1;
                    }
                } else {
                    current.push(c);
                    i += 1;
                }
            }
            ';' => {
                out.push(std::mem::take(&mut current));
                i += 1;
            }
            _ => {
                current.push(c);
                i += 1;
            }
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

// WHAT:  Uppercased bare words at parenthesis depth 0, with literals and comments removed.
fn top_level_words(statement: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut depth: i32 = 0;
    let chars: Vec<char> = statement.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let flush = |word: &mut String, words: &mut Vec<String>, depth: i32| {
        if !word.is_empty() {
            if depth == 0 {
                words.push(word.to_ascii_uppercase());
            }
            word.clear();
        }
    };
    while i < len {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            '\'' | '"' | '`' => {
                flush(&mut word, &mut words, depth);
                let quote = c;
                i += 1;
                while i < len {
                    if chars[i] == quote {
                        if chars.get(i + 1).copied() == Some(quote) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            '-' if next == Some('-') => {
                flush(&mut word, &mut words, depth);
                while i < len && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                flush(&mut word, &mut words, depth);
                i += 2;
                while i < len && !(chars[i] == '*' && chars.get(i + 1).copied() == Some('/')) {
                    i += 1;
                }
                i += 2;
            }
            '(' => {
                flush(&mut word, &mut words, depth);
                depth += 1;
                i += 1;
            }
            ')' => {
                flush(&mut word, &mut words, depth);
                depth -= 1;
                i += 1;
            }
            c if c.is_alphanumeric() || c == '_' => {
                word.push(c);
                i += 1;
            }
            _ => {
                flush(&mut word, &mut words, depth);
                i += 1;
            }
        }
    }
    flush(&mut word, &mut words, depth);
    words
}

const READ_LEADERS: &[&str] = &[
    "SELECT", "EXPLAIN", "SHOW", "VALUES", "TABLE", "DESCRIBE", "DESC", "BEGIN", "START", "COMMIT",
    "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET", "RESET", "DISCARD", "LISTEN", "UNLISTEN",
];
const DML_WRITERS: &[&str] = &["INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "UPSERT"];

fn classify_words(words: &[String]) -> (StatementKind, Option<String>) {
    let Some(first) = words.first().map(String::as_str) else {
        return (StatementKind::Read, None);
    };
    let has = |kw: &str| words.iter().any(|w| w == kw);
    match first {
        "DROP" => (StatementKind::Destructive, Some("DROP removes the object and its data.".into())),
        "TRUNCATE" => (StatementKind::Destructive, Some("TRUNCATE removes every row.".into())),
        "DELETE" if !has("WHERE") => {
            (StatementKind::Destructive, Some("DELETE without a WHERE clause removes every row.".into()))
        }
        "UPDATE" if !has("WHERE") => {
            (StatementKind::Destructive, Some("UPDATE without a WHERE clause rewrites every row.".into()))
        }
        "ALTER" if has("DROP") => {
            (StatementKind::Destructive, Some("ALTER ... DROP discards a column or constraint.".into()))
        }
        "WITH" => {
            if DML_WRITERS.iter().any(|kw| has(kw)) {
                (StatementKind::Write, None)
            } else {
                (StatementKind::Read, None)
            }
        }
        "PRAGMA" => {
            if words.len() > 2 {
                (StatementKind::Write, None)
            } else {
                (StatementKind::Read, None)
            }
        }
        kw if READ_LEADERS.contains(&kw) => (StatementKind::Read, None),
        _ => (StatementKind::Write, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(sql: &str) -> Vec<StatementKind> {
        classify(sql).into_iter().map(|s| s.kind).collect()
    }

    #[test]
    fn reads_are_reads() {
        assert_eq!(kinds("SELECT 1; select * from t where a = 'x;y'"), vec![StatementKind::Read, StatementKind::Read]);
        assert_eq!(kinds("WITH x AS (SELECT 1) SELECT * FROM x"), vec![StatementKind::Read]);
        assert_eq!(kinds("EXPLAIN ANALYZE SELECT 1"), vec![StatementKind::Read]);
        assert_eq!(kinds("PRAGMA table_info(users)"), vec![StatementKind::Read]);
    }

    #[test]
    fn writes_are_writes() {
        assert_eq!(kinds("INSERT INTO t VALUES (1)"), vec![StatementKind::Write]);
        assert_eq!(kinds("UPDATE t SET a = 1 WHERE id = 3"), vec![StatementKind::Write]);
        assert_eq!(kinds("DELETE FROM t WHERE id = 3"), vec![StatementKind::Write]);
        assert_eq!(kinds("WITH d AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM d)"), vec![StatementKind::Write]);
        assert_eq!(kinds("CREATE TABLE t (a int)"), vec![StatementKind::Write]);
        assert_eq!(kinds("PRAGMA journal_mode = WAL"), vec![StatementKind::Write]);
        assert_eq!(kinds("frobnicate everything"), vec![StatementKind::Write]);
    }

    #[test]
    fn destructive_needs_confirmation() {
        assert_eq!(kinds("DROP TABLE t"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("TRUNCATE t"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("DELETE FROM t"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("UPDATE t SET a = 1"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("ALTER TABLE t DROP COLUMN a"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("ALTER TABLE t ADD COLUMN a int"), vec![StatementKind::Write]);
    }

    #[test]
    fn where_inside_string_or_subquery_does_not_count() {
        assert_eq!(kinds("DELETE FROM t -- WHERE id = 1"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("UPDATE t SET note = 'WHERE'"), vec![StatementKind::Destructive]);
        assert_eq!(kinds("DELETE FROM t USING (SELECT 1 WHERE true) s"), vec![StatementKind::Destructive]);
    }

    #[test]
    fn splitter_respects_quotes_comments_and_dollar_quotes() {
        let parts = split_statements("select ';'; /* ; */ select 2; -- ;\nselect $$a;b$$; select 4");
        assert_eq!(parts.len(), 4);
        assert!(parts.get(2).is_some_and(|s| s.trim().ends_with("select $$a;b$$")), "comment stays attached: {parts:?}");
    }
}
