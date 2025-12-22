-- LDBC Official Query: IC-complex-6
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.162616
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (knownTag:Tag { name: $tagName })
-- WITH knownTag.id as knownTagId
-- 
-- MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
-- WHERE NOT person=friend
-- WITH
--     knownTagId,
--     collect(distinct friend) as friends
-- UNWIND friends as f
--     MATCH (f)<-[:HAS_CREATOR]-(post:Post),
--           (post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
--           (post)-[:HAS_TAG]->(tag:Tag)
--     WHERE NOT t = tag
--     WITH
--         tag.name as tagName,
--         count(post) as postCount
-- RETURN
--     tagName,
--     postCount
-- ORDER BY
--     postCount DESC,
--     tagName ASC
-- LIMIT 10

-- Generated ClickHouse SQL:
WITH RECURSIVE with_knownTagId_cte AS (SELECT 
      knownTag.id AS "knownTagId"
FROM ldbc.Tag AS knownTag
WHERE knownTag.name = $tagName
), 
vlp_cte24 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte24 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE start_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte24 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
)
    SELECT * FROM vlp_cte24
  )
),
vlp_cte25 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte25 AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE end_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte25 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
      AND end_node.id = $personId
)
    SELECT * FROM vlp_cte25
  )
)
SELECT 
      knownTag.url AS "knownTag.url", 
      knownTag.name AS "knownTag.name", 
      knownTag.id AS "knownTag.id"
FROM ldbc.Tag AS knownTag
WHERE (knownTag.name = $tagName AND NOT person = friend)

SETTINGS max_recursive_cte_evaluation_depth = 100

