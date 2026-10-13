WITH optional_o1 AS (
SELECT 
      v1.personId AS `v1__personId`, 
      v3.personId AS `v3__personId`, 
      v3.personId AS `p2_v3_personId`
FROM ldbc.person AS v1
JOIN ldbc.person_knows_person AS v2 ON v2.person1Id = v1.personId
JOIN ldbc.person AS v3 ON v2.person2Id = v3.personId
WHERE v1.personId = 1
)
SELECT 
      v0.commentId AS `c.commentId`, 
      v1.personId AS `p.personId`, 
      o1.p2_v3_personId AS `friend.personId`
FROM ldbc.comment AS v0
CROSS JOIN ldbc.person AS v1
LEFT JOIN optional_o1 AS o1 ON v1.personId = o1.v1__personId
WHERE (v0.commentId = 100 AND v1.personId = 1)
