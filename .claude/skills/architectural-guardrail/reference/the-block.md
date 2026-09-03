# The Block

## Contents
- Why it exists
- The fourteen steps
- Org scoping (the security rule)
- What belongs in the block vs the router
- Adding a new concern

## Why it exists

Every request that touches the backend passes through one procedure. Without it,
every feature prompt would have to restate auth, org scoping, permission checks,
rate limits, plan limits, usage counting, and audit logging — and would silently drop
some of them.

Because it is one place, adding a concern applies retroactively to every feature
already built.

## The fourteen steps

Order matters. Cheap rejections come before expensive ones.

| # | Step | Rejects when |
|---|---|---|
| 1 | Request setup | — builds ctx, starts the timer |
| 2 | Org scoping | no active organization |
| 3 | Onboarding gate | organization setup incomplete |
| 4 | Membership check | caller not a member of this org |
| 5 | Role gate | role below `requiredRole` |
| 6 | Permission gate | membership lacks `requiredPermission` |
| 7 | Path resolution | — derives the resource from the permission |
| 8 | Per-resource rate limit | resource ceiling exceeded |
| 9 | Plan limit gate | plan cap reached or feature not in tier |
| 10 | Feature flag gate | flag off for this org |
| 11 | Pagination | limit outside 1–100 |
| 12 | Context enrichment | — attaches orgId, userId, membership |
| 13 | Usage counter | — increments after success |
| 14 | Audit log | — writes on both success and error |

Steps 13 and 14 run *after* the handler, inside a try/catch, so failures are recorded
too.

## Org scoping (the security rule)

**The org ID comes from the session. Never from input. Never derived from other data.**

The failure this prevents: an in-app AI agent that works out the org ID by fetching
the current record and reading its org. Trick it into believing it is on another
tenant's record and you have cross-organization data leakage.

Because the block injects the real org ID, a prompt can claim any organization it
likes and the query still scopes correctly. This is what makes agentic features safe
to add later.

Bugs are solvable and users forgive them. A tenant data breach is neither.

## What belongs where

**In the block** — anything universal:
- authentication, membership, roles, permissions
- plan limits, feature flags, rate limits
- usage counting, audit logging, pagination bounds

**In the router** — anything specific to one feature:
- "an organization can have at most three owners"
- "at most three members may edit a site simultaneously"
- input shape validation via Zod
- orchestrating several service calls

If you find yourself writing the same check in two routers, it belongs in the block
or in a shared helper — not copied.

## Rate limiting lives one layer down

Rate limiting is on `baseProcedure`, not `protectedProcedure`, because the worst
rate-limit problems come from **public** endpoints. `protectedProcedure` chains
`baseProcedure`, so authenticated routes inherit it and public routes still get it.

Keep the chain shallow: one base, one public, one protected. Chain, don't multiply.

## Adding a new concern

1. Add the option to `ProtectedOptions` in `procedures/protected.ts`.
2. Insert the step in the numbered sequence, keeping cheap checks early.
3. If it needs configuration per resource, add the field to `resources.ts` so it is
   derived rather than passed at every call site.
4. Leave the numbered comments intact — they are how the next reader (human or model)
   understands the order.

Do not create a second protected procedure to hold the new concern.

## The gap

The block only protects what passes through it. Anything that renders without a tRPC
call — a vendor's prebuilt billing widget, for example — bypasses every rule.

Close it in `layout.tsx`: read the path, look up the required permission from the same
registry the sidebar uses, redirect on failure. Do not build a second procedure.

To find your own gaps, ask which pages render without making a tRPC call.
