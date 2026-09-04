# ObjectDB gateway contract for DB Free

ObjectDB has no public HTTP API; its server speaks a proprietary binary protocol
that only the ObjectDB JPA/JDO client understands. DB Free therefore talks to a
tiny **JPQL gateway** that you host next to your ObjectDB server (or embed in
the application that already owns the `EntityManagerFactory`). The gateway is
three endpoints; a 30-line servlet or Spring controller is enough.

## Connection form

| field | meaning |
|---|---|
| host | base URL of the gateway, e.g. `http://localhost:8090` or `https://app.example.com/objectdb` |
| port | fallback port when the host has none (default 8090) |
| database | path prefix under the base URL (default `/`), useful when one gateway fronts several databases (`/crm`, `/billing`) |
| username / secret | sent as HTTP Basic (both) or `Authorization: Bearer <secret>` (secret only) |

## Endpoints

All responses are `application/json`.

### `GET {base}/entities`

Returns the entity names known to the persistence unit.

```json
["Customer", "Order", "OrderLine"]
```

Objects with a `name` field are also accepted: `[{"name":"Customer"}]`.

### `GET {base}/entities/{name}/fields`

Returns the persistent fields of one entity, in declaration order.

```json
[
  {"name": "id",      "type": "long",    "id": true},
  {"name": "name",    "type": "String"},
  {"name": "created", "type": "Date"},
  {"name": "orders",  "type": "List<Order>", "nullable": true}
]
```

Only `name` is required. `id: true` marks the primary key (the grid uses it to
address rows); `type` and `nullable` are informational. A plain array of strings
(`["id","name"]`) is accepted too, in which case the first field is assumed to
be the key.

### `POST {base}/query`

```json
{"jpql": "SELECT c FROM Customer c WHERE c.name LIKE :p ORDER BY c.id", "max": 100, "first": 0, "params": {"p": "A%"}}
```

Runs the JPQL (`max` = `setMaxResults`, `first` = `setFirstResult`, `params`
= named parameters). The response is a JSON array:

- one object per entity when the select clause is an entity (`SELECT c FROM …`);
  serialise the entity's persistent fields (references as their id, collections
  as arrays of ids or omitted),
- one array per row for multi-select (`SELECT c.id, c.name FROM …`),
- one scalar per row for single-value selects (`SELECT COUNT(c) FROM …`).

```json
[{"id": 1, "name": "Acme", "created": "2024-01-02T03:04:05Z", "orders": [10, 11]}]
```

Optional: `{"rows": [...], "truncated": true}` instead of a bare array.

For `UPDATE` / `DELETE` JPQL return `{"affected": 3}`. DB Free refuses to send
those when the connection is marked read-only.

Errors: any non-2xx status with `{"error": "message"}` (the message is shown
verbatim in DB Free).

## Minimal servlet

```java
@WebServlet("/objectdb/*")
public class ObjectDbGateway extends HttpServlet {
    private static final EntityManagerFactory EMF = Persistence.createEntityManagerFactory("objectdb://localhost/app.odb;user=admin;password=admin");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String path = req.getPathInfo();
        Metamodel mm = EMF.getMetamodel();
        Object body;
        if ("/entities".equals(path)) {
            body = mm.getEntities().stream().map(EntityType::getName).sorted().toList();
        } else if (path.startsWith("/entities/") && path.endsWith("/fields")) {
            String name = path.substring(10, path.length() - 7);
            EntityType<?> e = mm.getEntities().stream().filter(t -> t.getName().equals(name)).findFirst().orElseThrow();
            body = e.getAttributes().stream().map(a -> Map.of("name", a.getName(), "type", a.getJavaType().getSimpleName(),
                    "id", a instanceof SingularAttribute<?, ?> s && s.isId())).toList();
        } else { res.setStatus(404); return; }
        res.setContentType("application/json"); JSON.writeValue(res.getOutputStream(), body);
    }

    @Override protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        Map<?, ?> in = JSON.readValue(req.getInputStream(), Map.class);
        EntityManager em = EMF.createEntityManager();
        try {
            Query q = em.createQuery((String) in.get("jpql"));
            if (in.get("max") != null) q.setMaxResults(((Number) in.get("max")).intValue());
            if (in.get("first") != null) q.setFirstResult(((Number) in.get("first")).intValue());
            ((Map<String, Object>) in.getOrDefault("params", Map.of())).forEach(q::setParameter);
            res.setContentType("application/json");
            JSON.writeValue(res.getOutputStream(), q.getResultList()); // Jackson serialises entities by their getters
        } catch (Exception ex) {
            res.setStatus(400); JSON.writeValue(res.getOutputStream(), Map.of("error", ex.getMessage()));
        } finally { em.close(); }
    }
}
```

Add `jackson-databind` and enable `@JsonIdentityInfo` (or a mixin) on entities
with cycles. That is the whole integration: DB Free discovers entities, shows
their fields, pages with `SELECT e FROM Name e ORDER BY … ` and lets you run
arbitrary JPQL in the query tab.
