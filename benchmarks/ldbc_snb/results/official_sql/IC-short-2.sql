-- LDBC Official Query: IC-short-2
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.178977
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
-- WITH
--  message,
--  message.id AS messageId,
--  message.creationDate AS messageCreationDate
-- ORDER BY messageCreationDate DESC, messageId ASC
-- LIMIT 10
-- MATCH (message)-[:REPLY_OF*0..]->(post:Post),
--       (post)-[:HAS_CREATOR]->(person)
-- RETURN
--  messageId,
--  coalesce(message.imageFile,message.content) AS messageContent,
--  messageCreationDate,
--  post.id AS postId,
--  person.id AS personId,
--  person.firstName AS personFirstName,
--  person.lastName AS personLastName
-- ORDER BY messageCreationDate DESC, messageId ASC

-- Generated ClickHouse SQL:
WITH RECURSIVE with_message_messageCreationDate_messageId_cte_1 AS (SELECT 
      message.browserUsed AS "message_browserUsed", 
      message.content AS "message_content", 
      message.creationDate AS "message_creationDate", 
      message.id AS "message_id", 
      message.length AS "message_length", 
      message.locationIP AS "message_locationIP", 
      message.id AS "messageId", 
      message.creationDate AS "messageCreationDate"
FROM ldbc.Comment AS message
INNER JOIN HAS_CREATOR AS t221 ON t221.from_id = message.id
INNER JOIN ldbc.Person AS t220 ON t220.id = t221.to_id
WHERE t220.id = $personId
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
), 
vlp_cte26 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte26 AS (
    SELECT 
        start_node.id as start_id,
        start_node.id as end_id,
        0 as hop_count,
        CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id] as path_nodes
    FROM ldbc.with_message_messageCreationDate_messageId_cte_1 AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.CommentId, rel.PostId)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF::Comment::Post']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte26 vp
    JOIN ldbc.Post AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Comment_replyOf_Post AS rel ON current_node.id = rel.CommentId
    JOIN ldbc.Post AS end_node ON rel.PostId = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.CommentId, rel.PostId))
)
    SELECT * FROM vlp_cte26
  )
)
SELECT 
      messageId AS "messageId", 
      coalesce(message.imageFile, message.content) AS "messageContent", 
      messageCreationDate AS "messageCreationDate", 
      post.id AS "postId", 
      person.id AS "personId", 
      person.firstName AS "personFirstName", 
      person.lastName AS "personLastName"
FROM vlp_cte26 AS vlp26
JOIN with_message_messageCreationDate_messageId_cte_1 AS message ON vlp26.start_id = message.id
JOIN ldbc.Post AS post ON vlp26.end_id = post.id
INNER JOIN ldbc.Post_hasCreator_Person AS t223 ON t222.PostId = t223.PostId
INNER JOIN ldbc.Person AS person ON person.id = t223.PersonId
ORDER BY messageCreationDate DESC, messageId ASC

SETTINGS max_recursive_cte_evaluation_depth = 100

