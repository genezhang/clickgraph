SELECT 
      c.commentId AS `c.commentId`, 
      p.personId AS `p.personId`
FROM ldbc.comment AS c
JOIN ldbc.person AS p ON 1 = 1
