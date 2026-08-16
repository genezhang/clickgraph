# GraphRAG scenarios — what an agent does that a vector store can't

Once an MCP-capable assistant is pointed at a ClickGraph / DeltaGraph Bolt
endpoint (see [`README.md`](./README.md)), it has two tools: `get_neo4j_schema`
and `read_neo4j_cypher`. That is enough to run a full **GraphRAG loop** — discover
the graph, translate a plain-English question into Cypher, traverse the warehouse,
and ground its answer in the rows that come back.

The scenarios below are an **escalating** set. The first is ordinary retrieval a
vector store could approximate. Everything after it depends on *traversal* —
following edges across the warehouse — which similarity search cannot do. Each was
run **live against the Databricks / DeltaGraph social graph** (Bolt `:7688`); the
results shown are the actual rows returned.

> The agent always starts by calling `get_neo4j_schema`, which returns the labels
> (`User`, `Post`), their properties, and the relationships
> (`FOLLOWS` User→User, `AUTHORED`/`LIKED` User→Post). Every Cypher below is
> written from that discovered schema — no hand-holding.

---

## 1 · Baseline retrieval — "Who are the most influential users?"

The warm-up: an aggregation a keyword or vector index could roughly answer too.

```cypher
MATCH (u:User)<-[:FOLLOWS]-(f:User)
RETURN u.name AS influencer, count(f) AS followers
ORDER BY followers DESC, influencer LIMIT 5
```

| influencer | followers |
|---|---|
| Rachel | 5 |
| Tina | 5 |
| Xander | 5 |
| Alice | 3 |
| Jack | 3 |

---

## 2 · Two-hop recommendation — "Who should Alice follow?"

Friend-of-friend, minus people Alice already follows, minus Alice herself. This is
the canonical graph move — and **no embedding of Alice's posts can produce it**,
because the answer lives in the *shape* of the follow graph, not in any text.

```cypher
MATCH (me:User {name:'Alice'})-[:FOLLOWS]->(f:User)-[:FOLLOWS]->(fof:User)
WHERE fof <> me AND NOT (me)-[:FOLLOWS]->(fof)
RETURN fof.name AS suggested, count(DISTINCT f) AS mutual_connections
ORDER BY mutual_connections DESC, suggested LIMIT 5
```

| suggested | mutual_connections |
|---|---|
| Ben | 1 |
| Henry | 1 |
| Victor | 1 |

> The `fof <> me` node-identity comparison is exactly what motivated fix
> [#1076](https://github.com/genezhang/clickgraph/pull/1076): it resolves to the
> schema's `user_id` column, not a literal `.id`, so it runs unchanged on both
> ClickHouse and Databricks.

---

## 3 · Cross-hop content grounding — "What is Alice's network talking about?"

Two joins — `Alice ─FOLLOWS→ author ─AUTHORED→ Post` — pull the *actual content*
the people Alice follows are publishing. This is GraphRAG in the literal sense: the
LLM's answer is grounded in warehouse rows reached by traversal, scoped to Alice's
neighborhood rather than the whole corpus.

```cypher
MATCH (me:User {name:'Alice'})-[:FOLLOWS]->(f:User)-[:AUTHORED]->(p:Post)
RETURN f.name AS author, p.content AS post
ORDER BY p.created_at DESC LIMIT 5
```

| author | post |
|---|---|
| Xander | Graph neural networks |
| Tina | Subqueries and comprehensions |
| Tina | UNION queries in Cypher |
| David | ClickHouse integration patterns |
| David | Building social networks with graphs |

An assistant answering *"summarize what my network is posting about"* now has
real, attributed, neighborhood-scoped source material — not a fuzzy top-k over
everything.

---

## 4 · Connection path — "How is Alice connected to Rachel? And to Ben?"

The question a vector store cannot even represent: *what is the chain of
relationships between two specific entities?*

**How far apart** (shortest-path length):

```cypher
MATCH path = shortestPath((a:User {name:'Alice'})-[:FOLLOWS*1..5]->(b:User {name:'Rachel'}))
RETURN length(path) AS hops
```

→ `hops = 4` — Alice reaches Rachel in four follow-hops (and *not* within three;
the bounded `*1..3` traversal returns empty, confirming it).

**The actual chain** to a nearer user:

```cypher
MATCH (a:User {name:'Alice'})-[:FOLLOWS]->(via:User)-[:FOLLOWS]->(b:User {name:'Ben'})
RETURN a.name AS from_user, via.name AS connected_through, b.name AS to_user
```

| from_user | connected_through | to_user |
|---|---|---|
| Alice | David | Ben |

> **Known limitation ([#1077](https://github.com/genezhang/clickgraph/issues/1077)):**
> reading the *node names along a path* via a comprehension —
> `RETURN [n IN nodes(path) | n.name]` — currently fails at planning
> (`ProjectionTagging: No table context for alias 'n'`) on **both** backends;
> over Bolt it surfaces as an opaque `50N42`. `length(path)` and explicit-hop
> chains (shown above) work today; full path readout is tracked in #1077.

---

## 5 · Engagement ranking — "Which posts are resonating, and who wrote them?"

A three-way traversal joining authorship and likes around each post — the kind of
relationship-aggregation that a graph answers directly and a document store makes
you pre-compute.

```cypher
MATCH (author:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker:User)
RETURN author.name AS author, p.content AS post, count(DISTINCT liker) AS likes
ORDER BY likes DESC, post LIMIT 5
```

| author | post | likes |
|---|---|---|
| Uma | Graph machine learning | 5 |
| Grace | Graph algorithms overview | 4 |
| Ben | Graph query languages comparison | 4 |
| Leo | Indexing strategies for graphs | 4 |
| Iris | Real-time graph analytics | 4 |

---

## The point

| Scenario | Traversal depth | Could a vector store do it? |
|---|---|---|
| 1 · Influencers | 1 hop, aggregate | Roughly |
| 2 · Recommendation | 2 hops + anti-join | **No** — answer is in graph shape |
| 3 · Content grounding | 2 hops, neighborhood-scoped | **No** — needs edge-scoping, not similarity |
| 4 · Connection path | variable-length | **No** — not representable as a vector |
| 5 · Engagement | 3-way join | **No** — relationship aggregation |

Same schema. Same warehouse tables. **Zero ETL, zero graph database.** The agent
speaks Bolt to ClickGraph/DeltaGraph, which translates Cypher to ClickHouse or
Spark SQL and runs it where the data already lives.

Swap `:7688` (Databricks/DeltaGraph) for `:7687` (ClickHouse/ClickGraph) and every
query above runs unchanged — the graph is a query-time view over the warehouse, not
a copy of it.

*All results verified live on 2026-08-16 against the DeltaGraph Databricks social
graph.*
