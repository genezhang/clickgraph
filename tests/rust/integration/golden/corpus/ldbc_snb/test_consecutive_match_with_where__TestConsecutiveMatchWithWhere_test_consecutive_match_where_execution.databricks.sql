SELECT 
      v0.commentId AS `commentId`, 
      v2.personId AS `creatorId`
FROM ldbc.comment AS v0
JOIN ldbc.comment AS v1 ON v1.commentId = v0.commentId
JOIN ldbc.person AS v2 ON v1.creatorId = v2.personId
WHERE v0.commentId = 100
LIMIT 5
-- column commentId: Value [("commentId", "commentId")]
-- column creatorId: Value [("creatorId", "creatorId")]