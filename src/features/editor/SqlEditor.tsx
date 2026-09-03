// SOT: sql-editor, codemirror-setup, editor-theme, schema-completion
import { useEffect, useRef } from "react";
import { EditorState, Compartment } from "@codemirror/state";
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightActiveLineGutter, drawSelection } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { autocompletion, closeBrackets, closeBracketsKeymap, completionKeymap } from "@codemirror/autocomplete";
import { HighlightStyle, syntaxHighlighting, bracketMatching } from "@codemirror/language";
import { sql, MSSQL, MySQL, MariaSQL, PostgreSQL, SQLite, StandardSQL, type SQLDialect, type SQLNamespace } from "@codemirror/lang-sql";
import { tags } from "@lezer/highlight";
import type { Engine } from "@/lib/bindings";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun: () => void;
  engine: Engine;
  schema: SQLNamespace;
  defaultSchema?: string | undefined;
}

// WHAT:  Theme built from the CSS tokens so light/dark follow the app.
// WHERE: src/styles/globals.css (HeroUI variables)
const theme = EditorView.theme({
  "&": { backgroundColor: "var(--background)", color: "var(--foreground)", height: "100%", fontSize: "13px" },
  ".cm-scroller": { fontFamily: "var(--font-mono)", lineHeight: "1.55" },
  ".cm-content": { caretColor: "var(--color-accent)", padding: "12px 0" },
  ".cm-line": { padding: "0 16px" },
  ".cm-gutters": { backgroundColor: "transparent", color: "var(--color-muted)", border: "none", paddingRight: "4px" },
  ".cm-activeLine": { backgroundColor: "var(--color-surface-hover)" },
  ".cm-activeLineGutter": { backgroundColor: "transparent", color: "var(--foreground)" },
  "&.cm-focused .cm-cursor": { borderLeftColor: "var(--color-accent)", borderLeftWidth: "2px" },
  "&.cm-focused .cm-selectionBackground, ::selection": { backgroundColor: "var(--color-selection)" },
  ".cm-panels": { backgroundColor: "var(--color-surface-elevated)", color: "var(--foreground)" },
  ".cm-tooltip": { backgroundColor: "var(--color-surface-elevated)", border: "1px solid var(--border)", color: "var(--foreground)" },
  ".cm-tooltip-autocomplete > ul > li[aria-selected]": { backgroundColor: "var(--color-surface-hover)", color: "var(--foreground)" },
});

const highlight = HighlightStyle.define([
  { tag: tags.keyword, color: "var(--color-syntax-keyword)", fontWeight: "600" },
  { tag: [tags.string, tags.special(tags.string)], color: "var(--color-syntax-string)" },
  { tag: [tags.number, tags.bool, tags.null], color: "var(--color-syntax-number)" },
  { tag: [tags.comment, tags.lineComment, tags.blockComment], color: "var(--color-syntax-comment)", fontStyle: "italic" },
  { tag: [tags.operator, tags.punctuation], color: "var(--color-syntax-operator)" },
  { tag: [tags.typeName, tags.className, tags.standard(tags.name)], color: "var(--color-syntax-type)" },
]);

function sqlConfig(engine: Engine, schema: SQLNamespace, defaultSchema: string | undefined) {
  return sql(defaultSchema === undefined ? { dialect: dialectFor(engine), schema } : { dialect: dialectFor(engine), schema, defaultSchema });
}

function dialectFor(engine: Engine): SQLDialect {
  switch (engine) {
    case "postgres":
    case "supabase":
    case "neon":
    case "timescaledb":
    case "questdb":
    case "pgvector":
    case "postgis":
    case "cockroachdb":
    case "yugabytedb":
    case "duckdb":
    case "ibm_ims":
    case "raima_rdm":
      return PostgreSQL;
    case "mysql":
    case "planetscale":
    case "tidb":
      return MySQL;
    case "mariadb":
      return MariaSQL;
    case "mssql":
      return MSSQL;
    case "sqlite":
    case "spatialite":
    case "libsql":
    case "val_town":
    case "cloudflare_d1":
      return SQLite;
    default:
      return StandardSQL;
  }
}

export function SqlEditor({ value, onChange, onRun, engine, schema, defaultSchema }: SqlEditorProps) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<EditorView | null>(null);
  const langCompartment = useRef(new Compartment());
  const onChangeRef = useRef(onChange);
  const onRunRef = useRef(onRun);
  useEffect(() => {
    onChangeRef.current = onChange;
    onRunRef.current = onRun;
  });

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(),
        highlightActiveLineGutter(),
        highlightActiveLine(),
        drawSelection(),
        history(),
        bracketMatching(),
        closeBrackets(),
        autocompletion(),
        keymap.of([
          { key: "Mod-Enter", run: () => { onRunRef.current(); return true; } },
          ...closeBracketsKeymap,
          ...defaultKeymap,
          ...historyKeymap,
          ...completionKeymap,
          indentWithTab,
        ]),
        langCompartment.current.of(sqlConfig(engine, schema, defaultSchema)),
        syntaxHighlighting(highlight),
        theme,
        EditorView.updateListener.of((update) => {
          if (update.docChanged) onChangeRef.current(update.state.doc.toString());
        }),
      ],
    });
    const view = new EditorView({ state, parent: host });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- mount once; value/schema sync below
  }, []);

  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    view.dispatch({ effects: langCompartment.current.reconfigure(sqlConfig(engine, schema, defaultSchema)) });
  }, [engine, schema, defaultSchema]);

  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const current = view.state.doc.toString();
    if (current !== value) {
      view.dispatch({ changes: { from: 0, to: current.length, insert: value } });
    }
  }, [value]);

  return <div ref={hostRef} className="h-full min-h-0 w-full overflow-hidden" />;
}
