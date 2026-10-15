# Outcome-Loop Recipes (copy-paste starters)

Same edge taxonomy as `people_outcome_graph.yaml`, three domains. Each recipe:
schema snippet + enrichment (serving) query + score query. The worked
queries on `people_outcome_graph.yaml` at the end are covered by translation
tests (`outcome_loop_example_tests.rs`) — they run without a database.

Conventions: decision table carries `propensity` + `policy_version`;
outcome table carries `weight` (0..1) + `resolved_at`; feedback table carries
`signal` (+1/-1). Nightly job pattern (all domains):

```sql
SELECT cohort, skill_or_item, avgWeighted(weight, propensity_weight) AS score,
       count() AS n
FROM outcome.joined_decisions_outcomes
WHERE resolved_at >= now() - INTERVAL 30 DAY
GROUP BY cohort, skill_or_item
```

Serve: `SELECT score FROM score_table WHERE cohort = {c} AND item = {i}`.

## Recipe 1 — Commerce (this repo's ecommerce schema shape)

Edges on `Customer -[:SHOWN]-> Product` (table `impressions`, adds
`propensity`, `policy_version`, `cohort`), `Customer -[:PURCHASED]-> Product`
(table `orders`, adds `weight` = normalized order value, `rec_id`).

Enrichment (top-k for this customer's cohort + personal recent):

```cypher
MATCH (c:Customer) WHERE c.customer_id = $uid
MATCH (c)-[s:SHOWN]->(p:Product)
RETURN p.product_id AS item, s.propensity AS p
ORDER BY s.created_at DESC LIMIT 50
```

Score read:

```cypher
MATCH (c:Customer)-[:IN_COHORT]->(h:Cohort)
MATCH (h)-[e:EFFECTIVE_FOR]->(p:Product)
RETURN p.product_id AS item, e.score AS score
ORDER BY e.score DESC LIMIT 10
```

## Recipe 2 — Support (suggest → resolve)

Edges: `Ticket -[:SUGGESTED]-> Article` (`suggestions`: `propensity`,
`policy_version`), `Ticket -[:RESOLVED_BY]-> Article` (`resolutions`:
`weight` = 1 if resolved without escalation else 0.3, `rec_id`).

Enrichment: recent suggestions for this account + cohort top articles for
the ticket category. Same nightly aggregation, `weight` = resolution rate.

## Recipe 3 — Ops (mitigate → recover)

Edges: `Incident -[:MITIGATED_BY]-> Runbook` (`mitigations`: `propensity`,
`policy_version`), `Incident -[:RECOVERED_VIA]-> Runbook` (`recoveries`:
`weight` = 1 if MTTR < SLO else scaled, `rec_id`).

Enrichment: runbooks that recovered same-service incidents in-cohort last
30d, blended with this service's own history. Reward includes `-cost`
(MTTR minutes) per §3 of the spec.

## Worked queries on people_outcome_graph.yaml (tested)

```cypher
MATCH (u:Person) WHERE u.person_id = $uid
MATCH (u)-[r:RECOMMENDED]->(s:Skill)
RETURN s.skill_id AS item, r.propensity AS p
ORDER BY r.created_at DESC LIMIT 50
```

```cypher
MATCH (u:Person)-[:HAS_SKILL]->(s:Skill)
WHERE u.person_id = $uid AND s.track = $track
RETURN s.title AS title
```

```cypher
MATCH (u:Person)-[o:OUTCOME]->(s:Skill)
WHERE u.person_id = $uid AND o.weight > 0.7
RETURN s.skill_id AS item, o.weight AS w
ORDER BY o.resolved_at DESC LIMIT 20
```
