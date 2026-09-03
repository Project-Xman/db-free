#!/usr/bin/env python3
"""Validate a codebase against the Architectural Guardrail.

Checks what lint cannot express cheaply. Run before typecheck: it is faster and
its messages are more specific.

    python check_guardrail.py src/
    python check_guardrail.py src/ --changed-only file1.ts file2.ts

Exit 0 = clean. Exit 1 = violations found. Exit 2 = bad usage.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Directories that identify each layer. Kept as a table so adding a layer is a
# one-line change rather than a new branch in every check.
SERVICE_DIR = "server/services"
ROUTER_DIR = "server/trpc/routers"
PROCEDURE_DIR = "server/trpc/procedures"

SKIP_DIRS = {"node_modules", ".next", "dist", "build", ".git", "generated"}

# Tailwind theme tokens are fine; raw colour values break light/dark mode.
HARDCODED_COLOR = re.compile(
    r"""(?x)
    (?:className|class)\s*=\s*["'`][^"'`]*
    \b(?:text|bg|border|ring|fill|stroke)-
    (?:white|black|
       (?:slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|
          emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\d{2,3})
    \b
    """
)
HEX_COLOR = re.compile(r"""(?:className|class)\s*=\s*["'`][^"'`]*\#[0-9a-fA-F]{3,8}\b""")

# A Prisma call whose argument block never mentions orgId.
PRISMA_CALL = re.compile(
    r"prisma\.(\w+)\.(findMany|findFirst|findUnique|update|updateMany|delete|deleteMany|count)\s*\("
)

TS_ESCAPE_HATCHES = (
    (re.compile(r":\s*any\b"), "`any` type"),
    (re.compile(r"\bas\s+any\b"), "`as any` cast"),
    (re.compile(r"@ts-ignore"), "@ts-ignore"),
    (re.compile(r"@ts-nocheck"), "@ts-nocheck"),
)


@dataclass
class Finding:
    path: Path
    line: int
    rule: str
    message: str

    def render(self, root: Path) -> str:
        try:
            shown = self.path.relative_to(root)
        except ValueError:
            shown = self.path
        return f"  {shown}:{self.line}\n      [{self.rule}] {self.message}"


def posix(path: Path) -> str:
    """Forward slashes always, so checks behave the same on Windows."""
    return path.as_posix()


def iter_sources(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.suffix not in {".ts", ".tsx"}:
            continue
        if path.name.endswith(".d.ts"):
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def find_block_end(text: str, open_index: int) -> int:
    """Index just past the matching close paren, or end of text.

    Counting rather than regex, because Prisma arguments nest objects several
    levels deep and a non-greedy match stops at the first close paren.
    """
    depth = 0
    for i in range(open_index, len(text)):
        char = text[i]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return len(text)


def line_of(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def check_file(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    rel = posix(path)
    lines = text.splitlines()

    # --- SOT keyword line -------------------------------------------------
    if "SOT:" not in text:
        findings.append(
            Finding(
                path,
                1,
                "sot",
                "No `// SOT:` keyword line. Add one at the top naming the source of "
                "truth this file holds, so grep -l can find it without reading it.",
            )
        )

    # --- server-only in services -----------------------------------------
    if SERVICE_DIR in rel or rel.endswith("server/db.ts"):
        first = next((ln.strip() for ln in lines if ln.strip()), "")
        if not first.startswith('import "server-only"') and not first.startswith(
            "import 'server-only'"
        ):
            findings.append(
                Finding(
                    path,
                    1,
                    "server-only",
                    'First statement must be `import "server-only";`. Without it this '
                    "file is bundled to the client and its functions can be invoked "
                    "directly, bypassing every check in the block.",
                )
            )

    # --- routers must use protectedProcedure ------------------------------
    if ROUTER_DIR in rel:
        if "publicProcedure" in text:
            idx = text.index("publicProcedure")
            findings.append(
                Finding(
                    path,
                    line_of(text, idx),
                    "block-bypass",
                    "Router uses publicProcedure. Every router endpoint must use "
                    "protectedProcedure so it passes through the block.",
                )
            )
        if "router(" in text and "protectedProcedure" not in text:
            findings.append(
                Finding(
                    path,
                    1,
                    "block-bypass",
                    "Router defines endpoints but never calls protectedProcedure.",
                )
            )
        for match in re.finditer(r"prisma\.", text):
            findings.append(
                Finding(
                    path,
                    line_of(text, match.start()),
                    "layer-violation",
                    "Routers never touch the database. Move this into "
                    "src/server/services/ and call the service from here.",
                )
            )
            break  # one report per file is enough to act on

        # An org ID arriving as input is the cross-tenant leak vector.
        if re.search(r"\b(orgId|organizationId)\s*:\s*z\.", text):
            match = re.search(r"\b(orgId|organizationId)\s*:\s*z\.", text)
            assert match is not None
            findings.append(
                Finding(
                    path,
                    line_of(text, match.start()),
                    "org-scoping",
                    "Org ID accepted as router input. It must come from ctx.orgId, "
                    "injected by the block from the session — never from the caller.",
                )
            )

    # --- services must scope every query to an org ------------------------
    if SERVICE_DIR in rel:
        for match in PRISMA_CALL.finditer(text):
            open_index = text.index("(", match.end() - 1)
            block = text[open_index : find_block_end(text, open_index)]
            if "orgId" not in block and "organizationId" not in block:
                findings.append(
                    Finding(
                        path,
                        line_of(text, match.start()),
                        "org-scoping",
                        f"prisma.{match.group(1)}.{match.group(2)}() has no orgId in "
                        "its filter. An id alone lets a caller reach another "
                        "organization's rows.",
                    )
                )

    # --- TypeScript escape hatches ----------------------------------------
    for pattern, label in TS_ESCAPE_HATCHES:
        match = pattern.search(text)
        if match:
            findings.append(
                Finding(
                    path,
                    line_of(text, match.start()),
                    "type-safety",
                    f"{label} found. A type you can bypass is not a guardrail — "
                    "derive the correct type instead.",
                )
            )

    # --- hardcoded colours -------------------------------------------------
    if path.suffix == ".tsx":
        for pattern, label in ((HARDCODED_COLOR, "palette class"), (HEX_COLOR, "hex value")):
            match = pattern.search(text)
            if match:
                findings.append(
                    Finding(
                        path,
                        line_of(text, match.start()),
                        "design-tokens",
                        f"Hardcoded colour ({label}). Use Tailwind theme tokens so "
                        "globals.css controls the palette and dark mode keeps working.",
                    )
                )

    # --- unresolved gaps ---------------------------------------------------
    for match in re.finditer(r"@guardrail-gap", text):
        findings.append(
            Finding(
                path,
                line_of(text, match.start()),
                "open-gap",
                "Deliberate gap still open. Resolve it or confirm it is intentional.",
            )
        )

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default="src", help="directory to scan")
    parser.add_argument(
        "--changed-only",
        nargs="*",
        default=None,
        metavar="FILE",
        help="check only these files (useful on a diff)",
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"error: {root} does not exist", file=sys.stderr)
        return 2

    targets = (
        [Path(f) for f in args.changed_only if Path(f).suffix in {".ts", ".tsx"}]
        if args.changed_only is not None
        else iter_sources(root)
    )

    findings: list[Finding] = []
    for path in targets:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        findings.extend(check_file(path, text))

    if not findings:
        print(f"Guardrail check passed — {len(targets)} file(s) clean.")
        return 0

    by_rule: dict[str, list[Finding]] = {}
    for finding in findings:
        by_rule.setdefault(finding.rule, []).append(finding)

    print(f"GUARDRAIL CHECK FAILED — {len(findings)} violation(s)\n")
    # Security-relevant rules first, so the important ones are not scrolled past.
    priority = ["org-scoping", "block-bypass", "server-only", "layer-violation"]
    order = priority + sorted(k for k in by_rule if k not in priority)

    for rule in order:
        if rule not in by_rule:
            continue
        print(f"{rule} ({len(by_rule[rule])})")
        for finding in by_rule[rule]:
            print(finding.render(root))
        print()

    print("Fix these and re-run before typecheck.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
