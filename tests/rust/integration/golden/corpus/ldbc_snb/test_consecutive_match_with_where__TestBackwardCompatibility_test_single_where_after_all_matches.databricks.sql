SELECT 
      v0.commentId AS `c.commentId`, 
      v1.personId AS `p.personId`
FROM ldbc.comment AS v0
CROSS JOIN ldbc.person AS v1
WHERE v0.commentId = 1

-- column c.commentId: Value [("c.commentId", "c.commentId")]
-- column p.personId: Value [("p.personId", "p.personId")]