# The Registry

## Contents
- What a resource entry contains
- Adding a resource
- How permissions are derived
- Plan limits and the UNLIMITED sentinel
- Upgrade messages
- Navigation

## What a resource entry contains

`src/lib/resources.ts` holds every gated resource. One object drives feature gates,
permission constants, plan limits, and the sidebar — so they cannot disagree.

| Field | Purpose |
|---|---|
| `name` | human-readable label |
| `description` | one line, used in settings UI |
| `limits` | per-plan numeric cap, keyed to `PlanKey` |
| `permissions` | which operations exist for this resource |
| `upgradeMessage` | `{next}` is replaced with the caller's next tier |
| `nav` | optional sidebar entry |

Because `limits` is `Record<PlanKey, Limit>`, a missing or misspelled plan key is a
compile error.

## Adding a resource

```ts
projects: {
  name: "Projects",
  description: "Workspaces containing tasks and files",
  limits: { free: 1, starter: 10, pro: 100, enterprise: UNLIMITED, portal: UNLIMITED },
  permissions: ["create", "read", "update", "delete"],
  upgradeMessage: "You've reached your project limit. Upgrade to {next} for more.",
  nav: { label: "Projects", href: "/dashboard/projects" },
},
```

That is the whole change. Permission strings, plan gating, upgrade copy, and the
sidebar entry all follow automatically.

## How permissions are derived

`src/lib/permissions.ts` builds a template-literal union over the registry:

```ts
export type PermissionKey = {
  [R in ResourceKey]: `${R}:${(typeof RESOURCES)[R]["permissions"][number]}`;
}[ResourceKey];
```

Hovering `PermissionKey` shows every permission in the app at once. Without this,
answering "what is the permission structure here?" means grepping the whole codebase.

**Removing an operation removes it everywhere.** `invitations` omits `update`, because
an invitation is created, read, or revoked — never edited. `permission("invitations",
"update")` is a compile error as a result.

`as const satisfies` is what makes this work: it validates the shape while preserving
literal types. A plain type annotation would widen `permissions` to `string[]` and the
derivation would collapse to `string`.

Because entries are narrowed to their literal types, resources that omit `nav`
genuinely have no such property. Narrow with `"nav" in definition` — reading
`definition.nav` directly will not compile.

## Plan limits and the sentinel

`-1` (`UNLIMITED`) means no cap. `0` means the feature is not in that tier at all,
which produces a different message from "you used them all".

A sentinel keeps the type numeric so comparisons stay simple. `null` or `Infinity`
would force a null check at every call site.

## Upgrade messages

Write `{next}` rather than naming a tier. `nextPlan()` walks `PLAN_ORDER` and
interpolates the caller's actual next step, so the same string works from any tier.

`portal` is excluded from `PLAN_ORDER` — it is an internal bypass tier, not a step on
a customer's upgrade path, so it resolves to a generic phrase instead.

## Navigation

`navItems()` derives the sidebar from the same registry that gates access. The layout
guard uses it too, matching longest-prefix-first so `/dashboard/billing/invoices`
resolves to `billing` rather than a shorter accidental match.

This is why the sidebar and the permission system can never drift apart.
