# Schema-mapping cost: generated SQL vs canonical graph views

**Question.** ClickGraph defines the graph→table mapping abstractly in the schema
YAML, and the translator inlines and optimizes that mapping at compile time.
A tempting simplification is to instead define standard `node_*` / `edge_*`
**SQL views** over the diverse physical tables, then translate Cypher against
those uniform views — letting the database optimizer untangle the physical
mapping at run time. That would shrink the translator a lot. Is it efficient
enough?

**Answer (on ClickHouse): no — the view layer loses optimizations the current
approach guarantees.** The cost is concentrated in three places, measured below
on 100k-row tables. `run.sh` is the reproducible harness; `setup.sql` builds the
physical tables + the canonical views. Numbers are from a local single-node
ClickHouse.

| Case | Signal | Generated (`cg`) | Canonical view | Penalty |
|---|---|---|---|---|
| Composite-id point join | `transfers` granules scanned | **3 / 12** | **12 / 12** (full) | 4× scan |
| FK-edge | base-table scans / joins | **2 / 1** | **3 / 2** | +1 scan, +1 join |
| VLP `*1..3` (composite) seed | rows read | **16,384** | **108,192** | 6.6× |
| VLP `*1..3` (composite) full | rows read | **116,384** | **208,192** | 1.8× |

## The headline case: skipping unnecessary joins (edge-collapse)

When a relationship is just a **foreign-key column on a node table** (FK-edge), or
the edge table *is* the node table, ClickGraph renders the traversal as a direct
join on the existing column — it does **not** introduce a separate edge relation:

```cypher
MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WHERE c.name = 'Alice' RETURN o.order_id
```
```sql
-- GENERATED: 2 base scans, 1 join — the FK column IS the edge
FROM customers_fk AS c
JOIN orders_fk    AS o ON c.customer_id = o.customer_id
WHERE c.name = 'Alice'
```

The canonical-view form models `edge_PLACED_BY` as a distinct relation
(`SELECT order_id AS start_id, customer_id AS end_id FROM orders_fk`), so the
plan becomes `node_Order ⋈ edge_PLACED_BY ⋈ node_Customer` — and ClickHouse
reads `orders_fk` **twice** (once as the node, once as the edge) with an extra
join. It does not collapse the redundant self-reference back into a single scan:

```
base-table scans — GENERATED: 2   VIEW: 3
```

This join-elimination is the clearest win of compile-time mapping: the translator
*knows* the edge and node share a table and emits one scan; the optimizer, handed
two views, cannot recover that.

## Why the other two cases diverge

- **Composite-id → synthetic-key joins.** A canonical single-column `id` over a
  multi-column key must be `concat(bank_id,'|',account_number)`. That synthetic
  key becomes the join key, which **breaks predicate propagation to the base
  primary key**: the generated SQL joins native columns, so `a1.bank_id = 1`
  reaches the `transfers` PK (`from_bank_id IN [1,1]` → 3/12 granules); the view's
  `concat` key severs it → full 12/12 scan.
- **VLP — partial.** Note ClickGraph's *own* VLP carries a synthetic `end_id` in
  the recursive frontier, so the *recursive* steps full-scan in both forms. The
  generated edge keeps its advantage only on the **anchored seed** (16k vs 108k)
  plus avoiding per-step view expansion — net 1.8× on a 3-hop query. (A place the
  current design could still improve: keep the frontier natively keyed.)

## Reframe

The abstract schema mapping *is* a view definition; the only question is **who
expands and optimizes it — our renderer at compile time, or the DB optimizer at
run time.** On ClickHouse (immature CBO, recursion-via-CTE, data-skipping over
native columns only) compile-time inlining wins, mostly via edge-collapse and
native-key joins. On a strong-CBO engine the gap would narrow; the composite-key
penalty is the most engine-independent.

Caveat: these are *schema-mapping* costs. Most ClickGraph translation bugs live
on the orthogonal *Cypher-feature × dialect* axis (alias scoping, undirected
unions, dialect quoting), which views would not simplify.

## Reproduce

```bash
# ClickHouse on :8123 (test_user/test_pass); cg built with --features databricks
docker exec <ch> clickhouse-client -u test_user --password test_pass \
  --multiquery --queries-file benchmarks/schema_mapping_cost/setup.sql
CG=/path/to/cg bash benchmarks/schema_mapping_cost/run.sh
```

## Not yet measured (extensions)

- **Polymorphic edges**: does the optimizer prune irrelevant `UNION` branches by
  label, or scan all physical tables behind `edge_*`?
- **Databricks/Spark**: stronger CBO + Delta data-skipping likely narrows the
  FK-edge gap; the composite synthetic-key penalty should persist.
