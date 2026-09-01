-- AGE (re-encoded node/edge) side of the re-encoding-tax micro-benchmark.
-- Loads the SAME data that bench_native.sql put into person/knows(FK) into
-- Apache AGE's internal label tables social."Person" / social."KNOWS", so the
-- only variable between the two sides is the representation (engine, hardware,
-- and data are identical). Run bench_native.sql FIRST — this reads from the
-- native `person` and `knows` tables to populate the graph.
--
-- Run:  docker exec age-bench psql -U bench -d bench -f /tmp/bench_age_load.sql

LOAD 'age'; SET search_path=ag_catalog,"$user",public;

-- ── Stage 1: fresh graph ──────────────────────────────────────────────────
-- (On a brand-new container the drop_graph errors harmlessly — nothing to drop —
--  then create_graph succeeds. On a reload it drops the prior graph first.)
SELECT drop_graph('social', true);
SELECT create_graph('social');

-- ── Stage 2: register the Person and KNOWS labels ─────────────────────────
-- AGE creates a label's backing table lazily on first element. Create one
-- throwaway vertex (registers Person) and one throwaway edge (registers KNOWS),
-- then TRUNCATE both so the bulk INSERT below owns all the data.
SELECT * FROM cypher('social', $$ CREATE (:Person {id:0, name:'seed', country:0}) $$) AS (v agtype);
SELECT * FROM cypher('social', $$ MATCH (a:Person) WITH a LIMIT 1 CREATE (a)-[:KNOWS]->(a) $$) AS (v agtype);
TRUNCATE social."Person";
TRUNCATE social."KNOWS";

-- ── Stage 3: bulk-load vertices and edges from the native tables ──────────
-- graphid = _graphid(label_id, local_id). Far faster than per-row Cypher CREATE.
INSERT INTO social."Person" (id, properties)
SELECT _graphid((SELECT id FROM ag_label WHERE name='Person' AND graph=(SELECT graphid FROM ag_graph WHERE name='social'))::int, p.id),
       agtype_build_map('id', p.id::agtype, 'name', ('"'||p.name||'"')::agtype, 'country', p.country::agtype)
FROM person p;

INSERT INTO social."KNOWS" (id, start_id, end_id, properties)
SELECT _graphid((SELECT id FROM ag_label WHERE name='KNOWS' AND graph=(SELECT graphid FROM ag_graph WHERE name='social'))::int, nextval('social."KNOWS_id_seq"')),
       _graphid((SELECT id FROM ag_label WHERE name='Person' AND graph=(SELECT graphid FROM ag_graph WHERE name='social'))::int, k.src),
       _graphid((SELECT id FROM ag_label WHERE name='Person' AND graph=(SELECT graphid FROM ag_graph WHERE name='social'))::int, k.dst),
       agtype_build_map()
FROM knows k;

-- ── Stage 4: indexes + stats, so the AGE side is fairly optimized ─────────
CREATE INDEX IF NOT EXISTS knows_age_start   ON social."KNOWS"(start_id);
CREATE INDEX IF NOT EXISTS knows_age_end     ON social."KNOWS"(end_id);
ANALYZE social."Person";
ANALYZE social."KNOWS";

-- ── Stage 5: sanity — counts should equal the native side ─────────────────
SELECT 'age_person' AS t, count(*) FROM social."Person"
UNION ALL SELECT 'age_knows', count(*) FROM social."KNOWS";

-- ── Stage 6: parity — AGE 2-hop from person 42 must equal the native count ─
SELECT * FROM cypher('social', $$
  MATCH (a:Person {id:42})-[:KNOWS]->()-[:KNOWS]->(c)
  RETURN count(c)
$$) AS (n agtype);
