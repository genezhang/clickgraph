SELECT 
      v0.commentId AS "c.commentId", 
      v2.personId AS "p.personId"
FROM ldbc.comment AS v0
JOIN ldbc.comment AS v1 ON v1.commentId = v0.commentId
JOIN ldbc.person AS v2 ON v1.creatorId = v2.personId
WHERE v0.commentId = 100

-- column c.commentId: Value [("c.commentId", "c.commentId")]
-- column p.personId: Value [("p.personId", "p.personId")]