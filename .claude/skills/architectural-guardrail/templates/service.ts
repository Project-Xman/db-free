import "server-only";

// SOT: <resource>-service, <resource>-data, <resource>-queries, database-<resource>
//
// WHAT:  Database access for <resource>. The only layer that talks to Prisma.
// WHY:   `server-only` must be the FIRST statement. Without it this file is bundled
//        to the client, where its functions can be invoked directly by anyone who
//        finds the bundle ID — skipping every permission check in the block.
// HOW:   Called from <resource>.router.ts only. Never imported by a component.
// WHERE: Schema in prisma/schema.prisma is the source of truth for these types.

import { prisma } from "@/server/db";

// Every function takes orgId explicitly. No query may escape its organization.
interface OrgScoped {
  orgId: string;
}

export async function listThings({
  orgId,
  cursor,
  limit,
}: OrgScoped & { cursor: string | null; limit: number }) {
  // Fetch one extra row to detect a next page without a second count query.
  const rows = await prisma.thing.findMany({
    where: { orgId },
    take: limit + 1,
    ...(cursor ? { cursor: { id: cursor }, skip: 1 } : {}),
    orderBy: { createdAt: "desc" },
  });

  const hasMore = rows.length > limit;
  return {
    items: hasMore ? rows.slice(0, limit) : rows,
    nextCursor: hasMore ? rows[limit - 1].id : null,
  };
}

export async function createThing({ orgId, name }: OrgScoped & { name: string }) {
  return prisma.thing.create({ data: { orgId, name } });
}

export async function updateThing({
  orgId,
  thingId,
  name,
}: OrgScoped & { thingId: string; name: string }) {
  // orgId in the where clause, not just the id: an id alone would let a caller
  // update a row belonging to a different organization.
  return prisma.thing.update({ where: { id: thingId, orgId }, data: { name } });
}

export async function deleteThing({ orgId, thingId }: OrgScoped & { thingId: string }) {
  return prisma.thing.delete({ where: { id: thingId, orgId } });
}
