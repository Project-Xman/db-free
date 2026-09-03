---
name: architectural-guardrail
description: Builds and reviews features inside the Architectural Guardrail — a source-of-truth registry, a protected-procedure block that every request passes through, server-only services, dynamically generated TypeScript, grep-first context discovery, and red-green prompting. Use this skill whenever adding, editing, or reviewing any feature, API endpoint, router, service, permission, plan limit, feature gate, or component in this codebase. Use it even when the request is just "build X", "add Y", or "fix Z" and says nothing about architecture. Also use it when auditing a diff, planning a complex multi-part feature, or searching for existing types, helpers, or constants.
---

# Architectural Guardrail

This codebase does not rely on documentation to stay production-grade. It relies on
**errors** — type errors, lint errors, compile errors — that surface while code is
being written. Your job is to build into that system, never alongside it.

## Before writing anything: grep first

Never create a type, function, constant, or component before searching for it.

```bash
./.claude/skills/architectural-guardrail/scripts/sot.sh <keyword>
```

Or directly:

```bash
grep -rl "SOT:.*<keyword>" src/
```

**Always `-l`.** It lists filenames only. `grep -rn` dumps file *contents* into
context and is the single largest source of wasted tokens here. Narrow to a filename
first, then read that one file.

Every file starts with a `// SOT:` line naming the source of truth it holds. When you
create a file, add one.

**Assume it already exists.** The types, helpers, and gates for this app are already
written. If you search, you will usually find them. Duplicating them is the most
common failure mode.

## The five rules

1. **Never invent architecture.** Verify the pattern exists in the codebase first. If
   it genuinely does not, ask before creating it.
2. **Every router endpoint uses `protectedProcedure`.** No exceptions, no alternate
   procedures for "simple" routes.
3. **Only services touch the database**, and every service file's first statement is
   `import "server-only";`.
4. **No `any`, no `unknown`, no `as` casts, no `@ts-ignore`.** A type you can bypass
   is not a guardrail.
5. **Never run git.** Do not read history, stage, commit, revert, or checkout. The
   user handles all git operations.

## The layers

```
request → protectedProcedure (the block) → router → service → Prisma → database
```

| Layer | Owns | Never does |
|---|---|---|
| Block (`procedures/protected.ts`) | auth, org scoping, permissions, limits, rate limits, usage, audit | feature-specific rules |
| Router (`trpc/routers/`) | business logic, Zod validation, orchestration | touch the database |
| Service (`server/services/`) | database access, `server-only` | hold business rules |
| Prisma (`server/db.ts`) | provider abstraction | — |

The block handles fourteen concerns so no prompt has to mention them. Full list and
step order: [reference/the-block.md](reference/the-block.md).

## Adding a feature — the workflow

Copy this checklist into your response and tick items off:

```
- [ ] 1. Grep for existing source of truth (-l only)
- [ ] 2. Add or extend the resource in src/lib/resources.ts
- [ ] 3. Write the service (server-only first line, orgId in every query)
- [ ] 4. Write the router (protectedProcedure, Zod input, calls the service)
- [ ] 5. Wire the client using the shared gate helper
- [ ] 6. Run the validator, then typecheck and lint
```

### Step 2 — the registry

Adding a resource to `src/lib/resources.ts` automatically produces its permission
strings, its plan limits, its upgrade copy, and its nav entry. Never hand-write a
permission list. Details: [reference/registry.md](reference/registry.md).

### Step 3 — the service

```ts
import "server-only";

// SOT: <resource>-service, <resource>-data, database-<resource>

export async function listThings({ orgId, ... }: { orgId: string }) {
  return prisma.thing.findMany({ where: { orgId } });
}
```

`orgId` goes in **every** where clause, including updates and deletes. An `id` alone
lets a caller reach another organization's row.

Template: [templates/service.ts](templates/service.ts).

### Step 4 — the router

```ts
export const thingRouter = router({
  list: protectedProcedure({
    requiredPermission: permission("things", "read"),
  })
    .input(paginationInput)
    .query(({ ctx, input }) => thingService.listThings({ orgId: ctx.orgId, ...input })),
});
```

`ctx.orgId` is injected by the block from the session. **Never accept an org ID as
input** and never derive one from other data — that is how cross-organization data
leaks happen.

Template: [templates/router.ts](templates/router.ts).

### Step 5 — the client

Use the shared gate helper rather than re-checking plan rules in the component. The
same function runs on both sides, so the modal the user sees and the error the server
throws cannot disagree.

## Inline context injection

Above each meaningful block, four lines:

```ts
// WHAT:  one line on what this does
// WHY:   one line on why it exists
// HOW:   one line on how it connects to the rest of the system
// WHERE: path to the source of truth it depends on
```

Top of a function or block, not every line. **Do not create or update a docs
folder** — inline comments are how context is delivered in this project.

## TypeScript

- Types are **derived**, never hand-written twice. Use `keyof`, template literals,
  `as const satisfies`.
- **Prisma types are the source of truth** for data shapes. Import the generated type
  and extend it; never redeclare a model.
- Shared types live in `src/lib/`, never inline in a feature file.
- All router inputs use Zod, because Zod throws.

## Components

- Search for a reusable component before creating one. Extend or compose; never
  duplicate.
- Copying a pattern from another component is the signal to globalize it instead.
- Global components in `src/components/global/`, route-specific in
  `<route>/_components/`.
- **Never hardcode colours** (`text-white`, hex values). Tailwind theme tokens only,
  or light/dark mode breaks.

## Before handing back

```bash
python .claude/skills/architectural-guardrail/scripts/check_guardrail.py src/
npx tsc --noEmit
npx eslint src/
```

Fix every error and re-run. Do not hand back a failing build, and never leave a
feature partly done with a "next steps" note for the hard part — finish it.

## Deliberate gaps

When the user asks you to defer part of a feature, leave a single source-of-truth
function where the logic will go and mark it so it announces itself:

```ts
// @guardrail-gap: email delivery not yet wired — see invite flow
console.warn("[gap] invite email not sent");
```

Never rely on memory to return to a gap. The marker is the point.

## Complex features

Break into features-of-their-own, then order so every prerequisite comes before the
thing that consumes it. A site builder that renders calendars needs the calendar
built **first**, even though the canvas feels like step one.

Planning method and red-green prompting: [reference/prompting.md](reference/prompting.md).

## Reference

- [reference/the-block.md](reference/the-block.md) — the fourteen steps, org scoping,
  what belongs in the block vs the router
- [reference/registry.md](reference/registry.md) — resource entries, derived
  permissions, plan limits, feature gates
- [reference/enforcement.md](reference/enforcement.md) — the ESLint and TypeScript
  rules, and what each one catches
- [reference/prompting.md](reference/prompting.md) — feature classification,
  prerequisite ordering, gap method, white-lie method
