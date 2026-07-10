SELECT 
      c.commentId AS `c.commentId`, 
      p.personId AS `p.personId`, 
      t0.person2Id AS `friend.personId`
FROM ldbc.person AS p
LEFT JOIN ldbc.person_knows_person AS t0 ON t0.person1Id = p.personId
WHERE (p.personId = 1 AND c.commentId = 100)
