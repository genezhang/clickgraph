-- LDBC Query: BI-10b
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.790520
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person {id: 14})-[:KNOWS*1..2]->(expert:Person)
-- MATCH (expert)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
-- WHERE tc.name = 'MusicalArtist'
-- RETURN DISTINCT
--     expert.id AS expertId,
--     expert.firstName AS firstName,
--     tag.name AS tagName,
--     count(post) AS postCount
-- ORDER BY postCount DESC, expertId
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte1 AS (
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
    WHERE start_node.id = 14
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte1 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
)
SELECT DISTINCT 
      expert.id AS "expertId", 
      expert.firstName AS "firstName", 
      tag.name AS "tagName", 
      count(post.id) AS "postCount"
FROM vlp_cte1 AS vlp1
JOIN ldbc.Person AS person ON vlp1.start_id = person.id
JOIN ldbc.Person AS expert ON vlp1.end_id = expert.id
INNER JOIN ldbc.Post_hasCreator_Person AS t29 ON t28.Person2Id = t29.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t30 ON t29.PostId = t30.PostId
INNER JOIN ldbc.Post AS post ON post.id = t29.PostId
INNER JOIN ldbc.Tag AS tag ON tag.id = t30.TagId
INNER JOIN ldbc.Tag_hasType_TagClass AS t31 ON t30.TagId = t31.TagId
INNER JOIN ldbc.TagClass AS tc ON tc.id = t31.TagClassId
GROUP BY expert.id, expert.firstName, tag.name
ORDER BY postCount DESC, expertId ASC
LIMIT  100
SETTINGS max_recursive_cte_evaluation_depth = 100

