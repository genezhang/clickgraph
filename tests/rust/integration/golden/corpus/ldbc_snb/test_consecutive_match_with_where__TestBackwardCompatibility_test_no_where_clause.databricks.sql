SELECT 
      c.commentId AS `c.commentId`, 
      p.personId AS `p.personId`
FROM ldbc.comment AS c
CROSS JOIN ldbc.person AS p
