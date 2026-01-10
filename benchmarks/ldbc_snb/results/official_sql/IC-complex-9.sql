-- LDBC Official Query: IC-complex-9
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.215800
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (root:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
-- WHERE NOT friend = root
-- WITH collect(distinct friend) as friends
-- UNWIND friends as friend
--     MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
--     WHERE message.creationDate < $maxDate
-- RETURN
--     friend.id AS personId,
--     friend.firstName AS personFirstName,
--     friend.lastName AS personLastName,
--     message.id AS commentOrPostId,
--     coalesce(message.content,message.imageFile) AS commentOrPostContent,
--     message.creationDate AS commentOrPostCreationDate
-- ORDER BY
--     commentOrPostCreationDate DESC,
--     message.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE with_friends_cte AS (SELECT 
      root.id AS "root.id", 
      root.birthday AS "root.birthday", 
      root.firstName AS "root.firstName", 
      root.gender AS "root.gender", 
      root.locationIP AS "root.locationIP", 
      root.browserUsed AS "root.browserUsed", 
      root.lastName AS "root.lastName", 
      root.creationDate AS "root.creationDate", 
      friend.id AS "friend.id", 
      friend.birthday AS "friend.birthday", 
      friend.firstName AS "friend.firstName", 
      friend.gender AS "friend.gender", 
      friend.locationIP AS "friend.locationIP", 
      friend.browserUsed AS "friend.browserUsed", 
      friend.lastName AS "friend.lastName", 
      friend.creationDate AS "friend.creationDate"
FROM vlp_cte10 AS vlp10
JOIN ldbc.Person AS root ON vlp10.start_id = root.from_id
JOIN ldbc.Person AS friend ON vlp10.end_id = friend.to_id
UNION ALL 
SELECT 
      friend.id AS "friend.id", 
      friend.birthday AS "friend.birthday", 
      friend.firstName AS "friend.firstName", 
      friend.gender AS "friend.gender", 
      friend.locationIP AS "friend.locationIP", 
      friend.browserUsed AS "friend.browserUsed", 
      friend.lastName AS "friend.lastName", 
      friend.creationDate AS "friend.creationDate", 
      root.id AS "root.id", 
      root.birthday AS "root.birthday", 
      root.firstName AS "root.firstName", 
      root.gender AS "root.gender", 
      root.locationIP AS "root.locationIP", 
      root.browserUsed AS "root.browserUsed", 
      root.lastName AS "root.lastName", 
      root.creationDate AS "root.creationDate"
FROM vlp_cte11 AS vlp11
JOIN ldbc.Person AS friend ON vlp11.start_id = friend.from_id
JOIN ldbc.Person AS root ON vlp11.end_id = root.to_id
WHERE root.id = $personId
), 
vlp_cte8 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte8 AS (
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
    FROM vlp_cte8 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
)
    SELECT * FROM vlp_cte8
  )
),
vlp_cte9 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte9 AS (
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
    FROM vlp_cte9 vp
    JOIN ldbc.Person AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Person_knows_Person AS rel ON current_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
      AND end_node.id = $personId
)
    SELECT * FROM vlp_cte9
  )
)
SELECT 
      friend.id AS "personId", 
      friend.firstName AS "personFirstName", 
      friend.lastName AS "personLastName", 
      message.id AS "commentOrPostId", 
      coalesce(message.content, message.imageFile) AS "commentOrPostContent", 
      message.creationDate AS "commentOrPostCreationDate"
FROM ldbc.Message AS message
INNER JOIN ldbc.Message_hasCreator_Person AS t87 ON t87.MessageId = message.id
INNER JOIN ldbc.Person_knows_Person AS t86 ON t86.Person2Id = t87.PersonId
INNER JOIN ldbc.Person AS friend ON friend.id = t87.PersonId
ARRAY JOIN friends AS friend
WHERE message.creationDate < $maxDate
ORDER BY commentOrPostCreationDate DESC, message.id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

