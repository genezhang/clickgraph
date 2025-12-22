-- LDBC Official Query: IC-complex-13
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.137832
-- Database: ldbc

-- Original Cypher Query:
-- MATCH
--     (person1:Person {id: $person1Id}),
--     (person2:Person {id: $person2Id}),
--     path = shortestPath((person1)-[:KNOWS*]-(person2))
-- RETURN
--     CASE path IS NULL
--         WHEN true THEN -1
--         ELSE length(path)
--     END AS shortestPathLength

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte16_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE start_node.id = $person1Id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte16_inner vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
),
vlp_cte16 AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_cte16_inner
    ) WHERE rn = 1
), 
vlp_cte17 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte17_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.from_id, rel.to_id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte17_inner vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Person AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
),
vlp_cte17_to_target AS (
    SELECT * FROM vlp_cte17_inner WHERE end_id = $person1Id
),
vlp_cte17 AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn
        FROM vlp_cte17_to_target
    ) WHERE rn = 1
)
    SELECT * FROM vlp_cte17
  )
)
SELECT 
      caseWithExpression(path IS NULL, true, 0 - 1, length(path)) AS "shortestPathLength"
FROM vlp_cte16 AS vlp16
JOIN ldbc.Person AS person1 ON vlp16.start_id = person1.id
JOIN ldbc.Person AS person2 ON vlp16.end_id = person2.id
UNION ALL 
SELECT 
      caseWithExpression(path IS NULL, true, 0 - 1, length(path)) AS "shortestPathLength"
FROM vlp_cte17 AS vlp17
JOIN ldbc.Person AS person2 ON vlp17.start_id = person2.id
JOIN ldbc.Person AS person1 ON vlp17.end_id = person1.id

