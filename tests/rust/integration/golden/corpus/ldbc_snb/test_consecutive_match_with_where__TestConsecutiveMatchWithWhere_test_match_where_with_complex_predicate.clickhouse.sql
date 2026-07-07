SELECT 
      c.commentId AS "c.commentId", 
      p.personId AS "p.personId"
FROM ldbc.comment AS c
WHERE ((c.commentId > 1000 AND c.commentId < 2000) AND p.personId > 100)
