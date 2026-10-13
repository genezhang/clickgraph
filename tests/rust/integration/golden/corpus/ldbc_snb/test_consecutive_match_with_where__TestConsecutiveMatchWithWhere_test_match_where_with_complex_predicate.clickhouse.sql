SELECT 
      v0.commentId AS "c.commentId", 
      v1.personId AS "p.personId"
FROM ldbc.comment AS v0
CROSS JOIN ldbc.person AS v1
WHERE ((v0.commentId > 1000 AND v0.commentId < 2000) AND v1.personId > 100)
