#!/usr/bin/env python3
"""Architectural Guardrail validator for DB Free (Rust core + TypeScript UI).

Checks what clippy / tsc / eslint cannot express cheaply:

  sot             every source file declares `SOT:` so grep -l can find it
  vendor-boundary sqlx / rusqlite / keyring / aes_gcm live in exactly one layer
  layering        commands never reach past services; services never touch tauri
  block-bypass    every #[tauri::command] passes through guard::
  ipc-boundary    only src/lib/ipc.ts may import @tauri-apps/api/core
  type-safety     no any / unknown (outside ipc.ts) / ts-ignore in the UI
  design-tokens   no hardcoded palette classes or hex colours in className
  open-gap        @guardrail-gap markers still present
  engine-facts    src/lib/engines.ts agrees with Rust on kind / form / defaultPort

    python3 scripts/guardrail.py            # scan src/ and src-tauri/src
    python3 scripts/guardrail.py --changed-only a.rs b.tsx

Exit 0 = clean. Exit 1 = violations. Exit 2 = bad usage.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUST_SRC = ROOT / "src-tauri" / "src"
TS_SRC = ROOT / "src"
SKIP_DIRS = {"node_modules", "dist", "target", ".git", "gen", "bindings"}

# crate -> the only files allowed to mention it
VENDOR_OWNERS = {
    "sqlx": ("integrations/postgres.rs", "integrations/mysql.rs"),
    "redis": ("integrations/redis.rs",),
    "mongodb": ("integrations/mongodb.rs",),
    # REST-style engines all go through integrations/http.rs; the adapters
    # listed here may still touch reqwest for engine-specific request shapes.
    "reqwest": (
        "integrations/http.rs",
        "integrations/clickhouse.rs",
        "integrations/libsql.rs",
        "integrations/cloudflare_d1.rs",
        "integrations/val_town.rs",
        "integrations/arangodb.rs",
        "integrations/basex.rs",
        "integrations/bigquery.rs",
        "integrations/chroma.rs",
        "integrations/couchdb.rs",
        "integrations/druid.rs",
        "integrations/dynamodb.rs",
        "integrations/elasticsearch.rs",
        "integrations/existdb.rs",
        "integrations/firestore.rs",
        "integrations/hbase.rs",
        "integrations/immudb.rs",
        "integrations/influxdb.rs",
        "integrations/meilisearch.rs",
        "integrations/milvus.rs",
        "integrations/objectdb.rs",
        "integrations/orientdb.rs",
        "integrations/pinecone.rs",
        "integrations/prometheus.rs",
        "integrations/qdrant.rs",
        "integrations/qldb.rs",
        "integrations/snowflake.rs",
        "integrations/sparql.rs",
        "integrations/surrealdb.rs",
        "integrations/tigergraph.rs",
        "integrations/typesense.rs",
        "integrations/weaviate.rs",
        "services/ai.rs",
    ),
    "tiberius": ("integrations/mssql.rs",),
    "scylla": ("integrations/cassandra.rs",),
    "neo4rs": ("integrations/neo4j.rs",),
    "rskafka": ("integrations/kafka.rs",),
    "duckdb": ("integrations/duckdb.rs",),
    "rocksdb": ("integrations/rocksdb.rs",),
    "oracle": ("integrations/oracle.rs",),
    "jsonwebtoken": ("integrations/gcp_auth.rs", "integrations/snowflake.rs"),
    "hmac": ("integrations/aws_sigv4.rs",),
    "sha2": ("integrations/aws_sigv4.rs", "integrations/snowflake.rs"),
    "csv": ("services/transfer.rs",),
    "rusqlite": ("store/", "integrations/sqlite.rs"),
    "keyring": ("adapters/keyring.rs",),
    "aes_gcm": ("adapters/crypto.rs",),
}

HARDCODED_COLOR = re.compile(
    r"""(?x)
    (?:className|class)\s*=\s*["'`{][^"'`]*
    \b(?:text|bg|border|ring|fill|stroke|from|to|via)-
    (?:white|black|
       (?:slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|
          emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\d{2,3})
    \b
    """
)
HEX_COLOR = re.compile(r"""(?:className|class)\s*=\s*["'`][^"'`]*\#[0-9a-fA-F]{3,8}\b""")

TS_ESCAPE_HATCHES = (
    (re.compile(r":\s*any\b"), "`any` type"),
    (re.compile(r"\bas\s+any\b"), "`as any` cast"),
    (re.compile(r"@ts-ignore"), "@ts-ignore"),
    (re.compile(r"@ts-nocheck"), "@ts-nocheck"),
    (re.compile(r"@ts-expect-error"), "@ts-expect-error"),
)


@dataclass
class Finding:
    path: Path
    line: int
    rule: str
    message: str

    def render(self) -> str:
        try:
            shown = self.path.relative_to(ROOT)
        except ValueError:
            shown = self.path
        return f"  {shown}:{self.line}\n      [{self.rule}] {self.message}"


def line_of(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def iter_sources() -> list[Path]:
    files: list[Path] = []
    for base, suffixes in ((RUST_SRC, {".rs"}), (TS_SRC, {".ts", ".tsx"})):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.suffix not in suffixes:
                continue
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            files.append(path)
    return sorted(files)


def check_rust(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    rel = path.relative_to(RUST_SRC).as_posix()

    for crate, owners in VENDOR_OWNERS.items():
        if not any(rel.startswith(o) for o in owners):
            # A leading `::`/identifier char means it is a path inside this crate
            # (e.g. adapters::keyring::...), not the vendor crate itself. The
            # integrations registry calls sibling modules that share a vendor
            # crate's name (`duckdb::connect(conn)`): only `connect(` is allowed there.
            match = re.search(rf"(?<![A-Za-z0-9_:]){crate}::(?!connect\()", text) or re.search(rf"\buse {crate}\b", text)
            if match:
                findings.append(Finding(path, line_of(text, match.start()), "vendor-boundary",
                    f"`{crate}` may only be used in {', '.join(owners)}. Map it to AppError there and expose a model type."))

    if rel.startswith("commands/"):
        for forbidden in ("crate::store", "crate::integrations::connect", "crate::adapters", "with_store("):
            idx = text.find(forbidden)
            if idx != -1:
                findings.append(Finding(path, line_of(text, idx), "layering",
                    f"Commands never reach `{forbidden}` directly. Call a function in src-tauri/src/services/ instead."))
        commands = len(re.findall(r"^\s*#\[tauri::command\]", text, flags=re.M))
        guarded = len(re.findall(r"guard::(?:local|session|statement)\(", text))
        if commands and guarded < commands:
            findings.append(Finding(path, 1, "block-bypass",
                f"{commands} command(s) but only {guarded} pass through guard::local / guard::session / guard::statement. Every command goes through the block."))

    if rel.startswith("services/") and re.search(r"\buse tauri\b|\btauri::", text):
        idx = re.search(r"\buse tauri\b|\btauri::", text).start()
        findings.append(Finding(path, line_of(text, idx), "layering",
            "Services never import tauri. They take &AppState / &SessionCtx and return model types."))

    return findings


def check_ts(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    rel = path.relative_to(TS_SRC).as_posix()
    is_ipc = rel == "lib/ipc.ts"

    if not is_ipc:
        for needle in ("@tauri-apps/api/core", "invoke("):
            idx = text.find(needle)
            if idx != -1:
                findings.append(Finding(path, line_of(text, idx), "ipc-boundary",
                    f"`{needle}` outside src/lib/ipc.ts. Call `ipc(...)` so every request passes through the client block."))
        match = re.search(r"\bunknown\b", text)
        if match:
            findings.append(Finding(path, line_of(text, match.start()), "type-safety",
                "`unknown` found. Only the IPC boundary (src/lib/ipc.ts) may narrow from unknown; derive the type from bindings instead."))

    for pattern, label in TS_ESCAPE_HATCHES:
        match = pattern.search(text)
        if match:
            findings.append(Finding(path, line_of(text, match.start()), "type-safety",
                f"{label} found. A type you can bypass is not a guardrail — derive the correct type instead."))

    if path.suffix == ".tsx":
        for pattern, label in ((HARDCODED_COLOR, "palette class"), (HEX_COLOR, "hex value")):
            match = pattern.search(text)
            if match:
                findings.append(Finding(path, line_of(text, match.start()), "design-tokens",
                    f"Hardcoded colour ({label}). Use the theme tokens in src/styles/globals.css so light/dark keep working."))
    return findings


def check_file(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    if "SOT:" not in text:
        findings.append(Finding(path, 1, "sot",
            "No `SOT:` keyword line. Add one at the top naming the source of truth this file holds."))
    if path.suffix == ".rs":
        findings.extend(check_rust(path, text))
    else:
        findings.extend(check_ts(path, text))
    for match in re.finditer(r"@guardrail-gap", text):
        findings.append(Finding(path, line_of(text, match.start()), "open-gap",
            "Deliberate gap still open. Resolve it or confirm it is intentional."))
    return findings




# WHAT:  The UI engine registry must agree with the Rust core on every field
#        Rust owns: the picker category, the connection-form kind and the
#        default port.
# WHY:   `src/lib/engines.ts` restates them for the UI. A disagreement means a
#        connection dialog with the wrong fields or a wrong prefilled port —
#        silent, and only visible when a user tries to connect. The values are
#        generated from `Engine::facts()` into EngineFacts.gen.ts by
#        `pnpm bindings`; this compares the two.
def check_engine_facts() -> list[Finding]:
    facts_path = TS_SRC / "lib" / "bindings" / "EngineFacts.gen.ts"
    engines_path = TS_SRC / "lib" / "engines.ts"
    if not facts_path.exists() or not engines_path.exists():
        return []
    facts_text = facts_path.read_text(encoding="utf-8")
    engines_text = engines_path.read_text(encoding="utf-8")

    rust: dict[str, dict[str, object]] = {}
    for name, body in re.findall(r"^  (\w+): (\{.*\}),$", facts_text, flags=re.M):
        try:
            rust[name] = json.loads(body)
        except json.JSONDecodeError:
            continue

    try:
        start = engines_text.index("export const ENGINES = {")
        end = engines_text.index("} satisfies Record<Engine, EngineMeta>")
    except ValueError:
        return []
    body = engines_text[start:end]

    findings: list[Finding] = []
    for name, facts in sorted(rust.items()):
        match = re.search(rf"^  {re.escape(name)}: (\w+)\((.*)\),$", body, flags=re.M)
        if not match:
            findings.append(Finding(engines_path, 1, "engine-facts",
                f"`{name}` is in Engine::ALL but missing from ENGINES in src/lib/engines.ts."))
            continue
        helper, entry = match.group(1), match.group(2)
        line = line_of(engines_text, start + match.start())

        kind = re.search(r'kind: "([a-z_]+)"', entry)
        if kind and kind.group(1) != facts["kind"]:
            findings.append(Finding(engines_path, line, "engine-facts",
                f"`{name}` kind is \"{kind.group(1)}\" but Engine::kind() says \"{facts['kind']}\". Run `pnpm bindings` and fix one side."))

        form = re.search(r'form: "([a-z_]+)"', entry)
        form_value = form.group(1) if form else {"server": "server", "http": "http_token", "file": "file"}.get(helper)
        if form_value is not None and form_value != facts["form"]:
            findings.append(Finding(engines_path, line, "engine-facts",
                f"`{name}` form is \"{form_value}\" but Engine::form() says \"{facts['form']}\"."))

        port = re.search(r"defaultPort: (null|\d+)", entry)
        port_value = None
        if port:
            port_value = None if port.group(1) == "null" else int(port.group(1))
        elif helper in {"http", "file"}:
            port_value = None
        if port and port_value != facts["defaultPort"]:
            findings.append(Finding(engines_path, line, "engine-facts",
                f"`{name}` defaultPort is {port_value} but Engine::default_port() says {facts['defaultPort']}."))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--changed-only", nargs="*", default=None, metavar="FILE")
    args = parser.parse_args()

    if args.changed_only is not None:
        targets = [Path(f).resolve() for f in args.changed_only if Path(f).suffix in {".rs", ".ts", ".tsx"}]
    else:
        targets = iter_sources()

    findings: list[Finding] = []
    for path in targets:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        findings.extend(check_file(path, text))

    # Cross-file rule: only meaningful on a full scan.
    if args.changed_only is None:
        findings.extend(check_engine_facts())

    # Deliberate gaps are reported every run but never fail the build: the
    # marker exists so deferred work announces itself, not to block shipping.
    gaps = [f for f in findings if f.rule == "open-gap"]
    findings = [f for f in findings if f.rule != "open-gap"]
    if gaps:
        print(f"open gaps ({len(gaps)}) — deliberate, tracked by @guardrail-gap markers:")
        for f in gaps:
            print(f.render())
        print()

    if not findings:
        print(f"Guardrail check passed — {len(targets)} file(s) clean.")
        return 0

    by_rule: dict[str, list[Finding]] = {}
    for f in findings:
        by_rule.setdefault(f.rule, []).append(f)
    print(f"GUARDRAIL CHECK FAILED — {len(findings)} violation(s)\n")
    priority = ["block-bypass", "vendor-boundary", "layering", "ipc-boundary"]
    for rule in priority + sorted(k for k in by_rule if k not in priority):
        if rule not in by_rule:
            continue
        print(f"{rule} ({len(by_rule[rule])})")
        for f in by_rule[rule]:
            print(f.render())
        print()
    print("Fix these and re-run before typecheck.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
