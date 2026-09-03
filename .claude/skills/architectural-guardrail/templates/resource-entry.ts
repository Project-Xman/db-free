// Paste into the RESOURCES object in src/lib/resources.ts.
//
// This one entry produces: the permission strings, the plan gating, the upgrade
// copy, and the sidebar link. Nothing else needs to be written by hand.

things: {
  name: "Things",
  description: "Short line shown in settings UI",

  // Every plan key must appear — a missing or misspelled key is a compile error.
  // 0 = not in this tier (different message from "you used them all").
  // UNLIMITED (-1) = no cap.
  limits: { free: 1, starter: 10, pro: 100, enterprise: UNLIMITED, portal: UNLIMITED },

  // Omit an operation to remove it from the permission union app-wide.
  // e.g. drop "update" and permission("things", "update") stops compiling.
  permissions: ["create", "read", "update", "delete"],

  // {next} is replaced with the caller's actual next tier at render time.
  upgradeMessage: "You've reached your limit. Upgrade to {next} for more.",

  // Omit for resources with no page of their own.
  nav: { label: "Things", href: "/dashboard/things" },
},
