// SOT: <resource>-router, <resource>-api, <resource>-endpoints
//
// WHAT:  <Resource> endpoints. Business logic and validation only.
// WHY:   Routers decide what is allowed for THIS feature. Everything universal —
//        auth, org scoping, permissions, limits, audit — already ran in the block.
// HOW:   Every endpoint uses protectedProcedure. Data access goes to the service.
// WHERE: Permissions from src/lib/permissions.ts. This file never touches Prisma.

import { z } from "zod";

import { permission } from "@/lib/permissions";
import { router } from "../init";
import { paginationInput, protectedProcedure } from "../procedures/protected";
import * as thingService from "@/server/services/thing.service";

// Zod because it throws. A wrong shape must fail, not pass through.
const thingIdInput = z.object({ thingId: z.string().min(1) });

export const thingRouter = router({
  list: protectedProcedure({
    requiredPermission: permission("things", "read"),
  })
    .input(paginationInput)
    .query(async ({ ctx, input }) =>
      // ctx.orgId is injected by the block from the session. NEVER take it as input.
      thingService.listThings({
        orgId: ctx.orgId,
        cursor: input.cursor ?? null,
        limit: input.limit,
      }),
    ),

  create: protectedProcedure({
    requiredPermission: permission("things", "create"),
    requiredRole: "admin",
    countsTowardUsage: true, // plan limit gate fires before this body runs
    rateLimit: { max: 20, windowMs: 60_000 },
  })
    .input(z.object({ name: z.string().min(1).max(120) }))
    .mutation(async ({ ctx, input }) =>
      thingService.createThing({ orgId: ctx.orgId, name: input.name }),
    ),

  update: protectedProcedure({
    requiredPermission: permission("things", "update"),
  })
    .input(thingIdInput.extend({ name: z.string().min(1).max(120) }))
    .mutation(async ({ ctx, input }) => {
      // Feature-specific business rules go HERE, not in the block.
      return thingService.updateThing({
        orgId: ctx.orgId,
        thingId: input.thingId,
        name: input.name,
      });
    }),

  remove: protectedProcedure({
    requiredPermission: permission("things", "delete"),
    requiredRole: "admin",
  })
    .input(thingIdInput)
    .mutation(async ({ ctx, input }) =>
      thingService.deleteThing({ orgId: ctx.orgId, thingId: input.thingId }),
    ),
});
