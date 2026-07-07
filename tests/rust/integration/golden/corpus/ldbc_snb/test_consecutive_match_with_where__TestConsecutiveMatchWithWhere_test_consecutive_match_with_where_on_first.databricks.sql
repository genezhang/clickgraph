SELECT 
      c.commentId AS `c.commentId`, 
      c.creatorId AS `p.personId`
FROM ldbc.comment AS c
WHERE c.commentId = 100
