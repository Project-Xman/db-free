# Planning and Prompting

## Contents
- Classifying a feature
- Simple features
- Complex features and prerequisite order
- Global-first thinking
- The gap method
- The white-lie method
- Stacking both

## Classifying a feature

| | Simple | Complex |
|---|---|---|
| Test | Says it in one sentence without "and then" | Cannot |
| Example | Lead lists, invite creation, a settings page | Website builder, automation builder |
| Approach | One-shot in plain English | Prerequisite chart, then iterate |

Watch the boundary. "Lead lists" is simple. "Lead lists that feed the automation
builder" is not — the automation builder is its own feature that merely appears in the
sentence.

## Simple features

State the big feature, then its small parts. The small parts are buttons and actions
inside the one feature, not features themselves.

```
Build lead lists.

A lead list is a saved, reusable filter over leads.
- Create a list from the current filter state
- Rename and delete a list
- Move leads between lists
- Show lead count per list in the sidebar
```

No permissions, no org scoping, no rate limits. The block covers those and TypeScript
will surface whatever options are still missing.

## Complex features and prerequisite order

Break into features-of-their-own, then number them so every prerequisite comes before
the thing that consumes it.

Unordered site builder: canvas, drag and drop, frames, styles, prebuilt components.

Most people put canvas first. Wrong — if the builder must render a calendar, the
calendar has to exist first.

```
1. calendar (the prebuilt component)
2. canvas
3. frames
4. drag and drop
5. styles
```

Build item 1 only, then re-plan.

## Global-first thinking

Before the canvas, the real first question is architecture, and inside it:

```
1. structure         what shape is the data
2. state management  where does it live
3. mutation          how does it change
```

Worked example: a store bounded to two sites at a time, because users rarely edit more
and holding many in memory is expensive. Elements as JSON with an id, a type, styles,
and nested children — because a rendering engine consumes JSON most easily. Special
elements carry a key mapping to prebuilt components, which is exactly why the calendar
came first.

If you are not technical, describe the processes in pseudocode and ask for an
architectural blueprint. The ordering is plain English and anyone can do it; lean on
the model for the technical shape, and let it challenge a design that feels right.

## The gap method

Deliberately leave something out so the right thing surfaces later.

**Bad** — fuses two jobs and costs you a reusable email service:

```
Build the team invitation system and send an email to the invited member.
```

**Good:**

```
Build invite creation only.

Do NOT wire up emails yet. Create a single source-of-truth service function
where the email logic will go later. For now, console.warn the message.
```

Never rely on memory to return to the gap — that is context again. Mark it:

| Marker | Good for |
|---|---|
| `console.warn` | quick, visible in dev |
| Dev-only toast | hard to miss while clicking through |
| `@guardrail-gap` comment | strongest — the lint rule surfaces it |

## The white-lie method

Your default assumption decides the model's first action.

| You imply | It does |
|---|---|
| Nothing exists yet | **Writes** |
| Something already exists | **Searches** |

So claim it exists, even when unsure.

**Bad**, however detailed:

```
Add a checkout form to the invoices page with Stripe. Match the other checkout forms.
```

**Good:**

```
We already have a source of truth for payments and checkout somewhere in this
application. Use that, and wire invoices into it.
```

Say *use it*, not *find it* — "find it" leaves room to conclude it is absent and start
writing.

There is no downside. Wrong, and it says so. Half-right, and it surfaces something you
forgot you built. Genuinely absent, and it usually proposes globalizing the thing you
should have globalized.

Two documented cases. A staleness bug suspected to be in the store rather than the
cache: asserting the bug existed and demanding a deep dive surfaced a singleton model
as the real root cause. And an editor that stayed slow after a migration: asserting
that two conflicting editor architectures were mixed, and asking for the claim to be
**verified first**, found the old implementation still wired in.

That "verify the claim first" step is worth keeping — it gives an honest exit while
still forcing the search.

## Stacking both

```
We already have a source of truth for payments and checkout somewhere in this
application. Verify that first, then use it and wire invoices into it.

Do NOT build the receipt email yet — leave a single source-of-truth function
where it will go, and console.warn for now.
```

Assert existence to force a search. Gap the second half to protect a future
globalization.
