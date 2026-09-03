# Enforcement

## Contents
- Why instructions are not enforcement
- The lint rules and what each catches
- TypeScript compiler flags
- The validator script
- Adding a new rule

## Why instructions are not enforcement

A rule written in CLAUDE.md can lose to a pattern in the codebase.

The documented failure: `server-only` was required in CLAUDE.md, but a handful of
older service files were missing it. The model pattern-matched those older files, and
roughly a fifth to a third of the app got built wrong while the rule sat in the config
the whole time.

Patterns beat instructions. Lint beats patterns, because the build fails.

So every rule that matters exists twice: once as an instruction (cheap, helps), once
as an error (decisive).

## The lint rules

### Rule 1 — `server-only` first in service files

```
Program > :first-child:not(ImportDeclaration[source.value="server-only"])
```

Reads as: the first node in the file that is *not* that exact import. If such a node
exists, the file is wrong.

**Catches:** a service file that would be bundled to the client, where its functions
can be invoked directly by anyone who finds the bundle ID — skipping every permission
check in the block.

Applies to `src/server/services/**` and `src/server/db.ts`.

### Rule 2 — no TypeScript escape hatches

Bans `any`, the `no-unsafe-*` family, and `@ts-ignore` / `@ts-expect-error` /
`@ts-nocheck`.

**Catches:** a guardrail type being bypassed rather than satisfied. A type you can
opt out of is not a guardrail.

### Rule 3 — routers never import the database

Blocks `@prisma/client` and `@/server/db` inside `src/server/trpc/routers/**`.

**Catches:** the router/service split quietly collapsing. Once one router queries
directly, the pattern is broken and the model will copy it.

### Rule 4 — vendor SDKs only inside the adapter layer

Blocks auth vendor imports everywhere except `src/server/adapters/**`.

**Catches:** vendor lock-in creeping back in. The adapter exists so swapping providers
is one file, not a refactor of every call site.

### Rule 5 — deliberate gaps must be visible

Warns on `@guardrail-gap`.

**Catches:** a deferred piece of work relying on memory to be finished. The gap method
only works if the gap announces itself.

## TypeScript compiler flags

Beyond `strict`, `tsconfig.json` enables:

| Flag | Catches |
|---|---|
| `noUncheckedIndexedAccess` | array and record lookups assumed to exist |
| `exactOptionalPropertyTypes` | `undefined` assigned to an optional field |
| `noImplicitReturns` | a code path that silently returns nothing |
| `noFallthroughCasesInSwitch` | a missing `break` |
| `noImplicitOverride` | a method accidentally shadowing a parent |

These matter more with generated code than hand-written code, because a model will
happily produce a plausible path that never returns.

## The validator

```bash
python .claude/skills/architectural-guardrail/scripts/check_guardrail.py src/
```

Checks what lint cannot express cheaply: missing `SOT:` lines, routers not using
`protectedProcedure`, org-unscoped Prisma queries, hardcoded colour values, and
unresolved gap markers.

Exit code 0 means clean. Non-zero prints the file, the line, and what to do.

Run it before typecheck — it is faster and its errors are more specific.

## Adding a new rule

Ask first: can the type system express this? If yes, put it there — compile errors
reach the model earliest and cost nothing at runtime.

If not, ask whether ESLint can express it as a selector. If not, add it to the
validator with a specific, actionable message. Vague messages ("invalid structure")
produce guessing; name the file, the line, and the fix.
