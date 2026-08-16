# GraphRAG scenarios — what an agent does that a vector store can't

Once an MCP-capable assistant is pointed at a ClickGraph / DeltaGraph Bolt
endpoint (see [`README.md`](./README.md)), it has two tools: `get_neo4j_schema`
and `read_neo4j_cypher`. That is enough to run a full **GraphRAG loop** — discover
the graph, translate a plain-English question into Cypher, traverse the warehouse,
and ground its answer in the rows that come back.

The scenarios below are an **escalating** set. The first is ordinary retrieval a
vector store could approximate. Everything after it depends on *traversal* —
following edges across the warehouse — which similarity search cannot do.
Scenarios 1–5 were run **live against the Databricks / DeltaGraph social graph**
(Bolt `:7688`); scenario 6 adds **hybrid vector + graph retrieval** on a
knowledge-base graph via the **ClickGraph / ClickHouse** endpoint (Bolt `:7687`),
where the vector-distance functions are native. In every case the results shown are
the actual rows returned.

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

**The whole chain, by name** — read the nodes along the shortest path:

```cypher
MATCH path = shortestPath((a:User {name:'Alice'})-[:FOLLOWS*1..5]->(b:User {name:'Rachel'}))
RETURN [n IN nodes(path) | n.name] AS connection_path, length(path) AS hops
```

| connection_path | hops |
|---|---|
| ["Alice", "David", "Henry", "Sam", "Rachel"] | 4 |

Alice reaches Rachel in four follow-hops, *through David → Henry → Sam* — a chain a
vector store cannot represent, let alone read back in order. The names come out in
traversal order because the property is materialized as a parallel array carried
alongside the path during the recursive traversal.

> Reading node properties along a path (`[n IN nodes(path) | n.name]`) shipped in
> [#1079](https://github.com/genezhang/clickgraph/pull/1079) — it was the third real
> bug this MCP demo surfaced. Verified live on both ClickHouse and Databricks.

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

## 6 · Hybrid retrieval — "Semantic entry, then graph expansion"

The one that puts vector search and the graph in the *same loop*. A pure vector
store retrieves by similarity and stops. Here the agent uses similarity only to
find the **entry point**, then the graph does what similarity cannot: traverse the
neighborhood, read the connection back **by name**, and re-rank grounded results.

Run against a knowledge-base graph (`Topic ─RELATED_TO→ Topic`,
`Topic ─DISCUSSES→ Document`) where both node types carry 128-dim embeddings. The
user's question ≈ *"deep learning that spans machine learning and NLP"*, encoded as
the centroid of the ML + NLP topic vectors.

**Step 1 — VECTOR: what is the question even about?** Rank topics by cosine
similarity to the query embedding.

```cypher
MATCH (t:Topic)
RETURN t.name AS topic, round(1 - cosineDistance(t.embedding, $q), 4) AS similarity
ORDER BY similarity DESC LIMIT 3
```

| topic | similarity |
|---|---|
| Natural Language Processing | 0.6844 |
| Machine Learning | 0.6844 |
| Databases | −0.0001 |

The two AI topics surface; unrelated topics sit at ~0. `cosineDistance` is
ClickHouse-native. **A vector store stops here.**

**Step 2 — GRAPH: how does that topic connect, read by name?** (the path readout
shipped in [#1079](https://github.com/genezhang/clickgraph/pull/1079))

```cypher
MATCH path = (start:Topic {name:'Machine Learning'})-[:RELATED_TO*1..2]->(t2:Topic)
RETURN [n IN nodes(path) | n.name] AS topic_chain, length(path) AS hops
ORDER BY hops, topic_chain
```

| topic_chain | hops |
|---|---|
| ["Machine Learning", "Natural Language Processing"] | 1 |
| ["Machine Learning", "Computer Vision"] | 1 |
| ["Machine Learning", "Natural Language Processing", "Computer Vision"] | 2 |

**Step 3 — HYBRID: of the documents in that neighborhood, which actually answer
the question?** The graph scopes the candidate set; the vector re-ranks it.

```cypher
MATCH (t:Topic)-[:DISCUSSES]->(d:Document)
WHERE t.name IN ['Machine Learning','Natural Language Processing','Computer Vision']
RETURN d.title AS document, t.name AS via_topic,
       round(1 - cosineDistance(d.embedding, $q), 4) AS similarity
ORDER BY similarity DESC LIMIT 5
```

| document | via_topic | similarity |
|---|---|---|
| Introduction to Neural Networks | Machine Learning | 0.6844 |
| Transformer Architecture Explained | Natural Language Processing | 0.6844 |
| Object Detection with YOLO | Computer Vision | −0.0016 |

The two foundational ML/NLP papers float to the top; the off-topic CV paper sinks.
Neither retrieval mode does this alone: **vectors find the entry point, the graph
traverses and explains the connection, then vectors re-rank the grounded
neighborhood.** That is GraphRAG that is both *relevant* and *explainable*.

> **Scope note.** Vector distance functions (`cosineDistance`, `L2Distance`,
> `dotProduct`, `gds.similarity.*`, `vector.similarity.*`) and the
> `CALL db.index.vector.queryNodes(...)` procedure are **ClickHouse-native**; this
> scenario runs on the ClickGraph/ClickHouse endpoint. Vector and traversal are
> kept as **separate agentic steps** on purpose — fusing them into one query
> (`… -[:RELATED_TO*1..2]->(t2)-[:DISCUSSES]->(d) … cosineDistance(…)`) can hit an
> intermittent planner bug tracked in
> [#1081](https://github.com/genezhang/clickgraph/issues/1081), which is
> independent of the vector functions themselves.

---

## The point

| Scenario | Traversal depth | Could a vector store do it? |
|---|---|---|
| 1 · Influencers | 1 hop, aggregate | Roughly |
| 2 · Recommendation | 2 hops + anti-join | **No** — answer is in graph shape |
| 3 · Content grounding | 2 hops, neighborhood-scoped | **No** — needs edge-scoping, not similarity |
| 4 · Connection path | variable-length | **No** — not representable as a vector |
| 5 · Engagement | 3-way join | **No** — relationship aggregation |
| 6 · Hybrid vector + graph | vector seed → traverse → re-rank | **Only step 1** — traversal & explanation need the graph |

Same schema. Same warehouse tables. **Zero ETL, zero graph database.** The agent
speaks Bolt to ClickGraph/DeltaGraph, which translates Cypher to ClickHouse or
Spark SQL and runs it where the data already lives.

Swap `:7688` (Databricks/DeltaGraph) for `:7687` (ClickHouse/ClickGraph) and every
query above runs unchanged — the graph is a query-time view over the warehouse, not
a copy of it.

*All results verified live on 2026-08-16 against the DeltaGraph Databricks social
graph.*
