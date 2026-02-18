-- LDBC Official Query: BI-bi-5
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.658572
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
-- OPTIONAL MATCH (message)<-[likes:LIKES]-(:Person)
-- WITH person, message, count(likes) AS likeCount
-- OPTIONAL MATCH (message)<-[:REPLY_OF]-(reply:Comment)
-- WITH person, message, likeCount, count(reply) AS replyCount
-- WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
-- RETURN
--   person.id,
--   replyCount,
--   likeCount,
--   messageCount,
--   1*messageCount + 2*replyCount + 10*likeCount AS score
-- ORDER BY
--   score DESC,
--   person.id ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH with_likeCount_message_person_cte_1 AS (SELECT 
      person.id AS "p6_person_id", 
      message.id AS "p7_message_id", 
      count(*) AS "likeCount"
FROM ldbc.Person AS t62
INNER JOIN ldbc.Message_hasCreator_Person AS t61 ON t61.MessageId = message.MessageId
INNER JOIN ldbc.Person AS person ON person.id = t61.PersonId
LEFT JOIN ldbc.Person_likes_Message AS likes ON likes.PersonId = t62.id
LEFT JOIN ldbc.Message_hasTag_Tag AS message ON message.MessageId = likes.MessageId
WHERE tag.name = $tag
GROUP BY person.id, message.id
), 
with_likeCount_message_person_replyCount_cte_1 AS (SELECT 
      likeCount_message_person.p6_person_id AS "p6_person_id", 
      likeCount_message_person.p7_message_id AS "p7_message_id", 
      anyLast(likeCount_message_person.likeCount) AS "likeCount", 
      count(reply.id) AS "replyCount"
FROM ldbc.Comment AS reply
LEFT JOIN ldbc.Comment_replyOf_Message AS t63 ON t63.CommentId = reply.id
LEFT JOIN with_likeCount_message_person_cte_1 AS likeCount_message_person ON likeCount_message_person.p6_person_id = t63.MessageId
GROUP BY likeCount_message_person.p6_person_id
), 
with_likeCount_messageCount_person_replyCount_cte_1 AS (SELECT 
      likeCount_message_person_replyCount.p6_person_id AS "p6_person_id", 
      count(likeCount_message_person.p7_message_id) AS "messageCount", 
      sum(likeCount_message_person.likeCount) AS "likeCount", 
      sum(likeCount_message_person.replyCount) AS "replyCount"
FROM with_likeCount_message_person_replyCount_cte_1 AS likeCount_message_person_replyCount
)
SELECT 
      likeCount.p6_person_id AS "person.id", 
      likeCount.replyCount AS "replyCount", 
      likeCount.likeCount AS "likeCount", 
      likeCount.messageCount AS "messageCount", 
      1 * likeCount.messageCount + 2 * likeCount.replyCount + 10 * likeCount.likeCount AS "score"
FROM with_likeCount_messageCount_person_replyCount_cte_1 AS likeCount
INNER JOIN ldbc.Message_hasCreator_Person AS t61 ON t61.MessageId = t60.MessageId
INNER JOIN ldbc.Comment_replyOf_Message AS t63 ON t63.MessageId = likes.PostId
ORDER BY score DESC, likeCount.p6_person_id ASC
LIMIT  100
