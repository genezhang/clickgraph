SELECT 
      v0.commentId AS `c.commentId`, 
      v1.personId AS `p.personId`
FROM ldbc.comment AS v0
CROSS JOIN ldbc.person AS v1
