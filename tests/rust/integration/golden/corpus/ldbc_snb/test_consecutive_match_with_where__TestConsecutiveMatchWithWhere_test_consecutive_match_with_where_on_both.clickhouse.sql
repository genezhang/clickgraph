SELECT 
      c.commentId AS "c.commentId", 
      p.personId AS "p.personId"
FROM ldbc.comment AS c
WHERE (c.commentId = 1 AND p.personId = 2)
