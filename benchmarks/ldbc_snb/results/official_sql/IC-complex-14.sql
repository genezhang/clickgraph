-- LDBC Official Query: IC-complex-14
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.141633
-- Database: ldbc

-- Original Cypher Query:
-- MATCH
--     path = shortestPath((person1 {id: $person1Id})-[:KNOWS*]-(person2 {id: $person2Id}))
-- 
-- WITH 42 AS dummy
-- 
-- MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
-- CALL gds.graph.project.cypher(
--   apoc.create.uuidBase64(),
--   'MATCH (p:Person) RETURN id(p) AS id',
--   'MATCH
--       (pA:Person)-[knows:KNOWS]-(pB:Person),
--       (pA)<-[:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[:HAS_CREATOR]->(pB)
--     WITH
--       id(pA) AS source,
--       id(pB) AS target,
--       count(r) AS numInteractions
--     RETURN
--       source,
--       target,
--       CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight
--   '
-- )
-- YIELD graphName
-- 
-- WITH person1, person2, graphName
-- 
-- CALL gds.shortestPath.dijkstra.stream(
--     graphName, {sourceNode: person1, targetNode: person2, relationshipWeightProperty: 'weight'}
-- )
-- YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
-- 
-- WITH path, totalCost, graphName
-- 
-- CALL gds.graph.drop(graphName, false)
-- YIELD graphName as graphNameremoved
-- 
-- RETURN [person IN nodes(path) | person.id] AS personIdsInPath, totalCost AS pathWeight
-- LIMIT 1

-- Generated ClickHouse SQL:
WITH RECURSIVE with_dummy_cte AS (SELECT 
      start_node.gender AS "person1.gender", 
      start_node.locationIP AS "person1.locationIP", 
      start_node.birthday AS "person1.birthday", 
      start_node.lastName AS "person1.lastName", 
      start_node.id AS "person1.id", 
      start_node.browserUsed AS "person1.browserUsed", 
      start_node.firstName AS "person1.firstName", 
      start_node.creationDate AS "person1.creationDate", 
      end_node.gender AS "person2.gender", 
      end_node.locationIP AS "person2.locationIP", 
      end_node.birthday AS "person2.birthday", 
      end_node.lastName AS "person2.lastName", 
      end_node.id AS "person2.id", 
      end_node.browserUsed AS "person2.browserUsed", 
      end_node.firstName AS "person2.firstName", 
      end_node.creationDate AS "person2.creationDate"
FROM vlp_cte20 AS vlp20
JOIN ldbc.Person AS person1 ON vlp20.start_id = person1.id
JOIN ldbc.Person AS person2 ON vlp20.end_id = person2.id
UNION ALL 
SELECT 
      start_node.gender AS "person2.gender", 
      start_node.locationIP AS "person2.locationIP", 
      start_node.birthday AS "person2.birthday", 
      start_node.lastName AS "person2.lastName", 
      start_node.id AS "person2.id", 
      start_node.browserUsed AS "person2.browserUsed", 
      start_node.firstName AS "person2.firstName", 
      start_node.creationDate AS "person2.creationDate", 
      end_node.gender AS "person1.gender", 
      end_node.locationIP AS "person1.locationIP", 
      end_node.birthday AS "person1.birthday", 
      end_node.lastName AS "person1.lastName", 
      end_node.id AS "person1.id", 
      end_node.browserUsed AS "person1.browserUsed", 
      end_node.firstName AS "person1.firstName", 
      end_node.creationDate AS "person1.creationDate"
FROM vlp_cte21 AS vlp21
JOIN ldbc.Person AS person2 ON vlp21.start_id = person2.id
JOIN ldbc.Person AS person1 ON vlp21.end_id = person1.id
), 
vlp_cte18 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte18_inner AS (
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
    WHERE start_node.id = $person1Id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte18_inner vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_cte18_to_target AS (
    SELECT * FROM vlp_cte18_inner WHERE end_id = $person2Id
),
vlp_cte18 AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn
        FROM vlp_cte18_to_target
    ) WHERE rn = 1
)
    SELECT * FROM vlp_cte18
  )
),
vlp_cte19 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte19_inner AS (
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
    WHERE start_node.id = $person2Id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte19_inner vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 5
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_cte19_to_target AS (
    SELECT * FROM vlp_cte19_inner WHERE end_id = $person1Id
),
vlp_cte19 AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn
        FROM vlp_cte19_to_target
    ) WHERE rn = 1
)
    SELECT * FROM vlp_cte19
  )
)
SELECT 
      person1.gender AS "person1.gender", 
      person1.locationIP AS "person1.locationIP", 
      person1.birthday AS "person1.birthday", 
      person1.lastName AS "person1.lastName", 
      person1.id AS "person1.id", 
      person1.browserUsed AS "person1.browserUsed", 
      person1.firstName AS "person1.firstName", 
      person1.creationDate AS "person1.creationDate", 
      person2.gender AS "person2.gender", 
      person2.locationIP AS "person2.locationIP", 
      person2.birthday AS "person2.birthday", 
      person2.lastName AS "person2.lastName", 
      person2.id AS "person2.id", 
      person2.browserUsed AS "person2.browserUsed", 
      person2.firstName AS "person2.firstName", 
      person2.creationDate AS "person2.creationDate"
FROM ldbc.Person AS person1
INNER JOIN ldbc.Person AS person2
WHERE (person1.id = $person1Id AND person2.id = $person2Id)

SETTINGS max_recursive_cte_evaluation_depth = 100

