SELECT 
      c.commentId AS "commentId", 
      c.creatorId AS "creatorId"
FROM ldbc.comment AS c
WHERE c.commentId = 100
LIMIT 5