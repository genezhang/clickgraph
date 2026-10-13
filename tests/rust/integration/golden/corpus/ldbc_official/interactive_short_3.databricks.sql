WITH v1_both AS (
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person1Id AS `__cg_start_0`, 
      e.Person2Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person2Id AS `__cg_start_0`, 
      e.Person1Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
)
SELECT 
      v2.id AS `personId`, 
      v2.firstName AS `firstName`, 
      v2.lastName AS `lastName`, 
      v1.creationDate AS `friendshipCreationDate`
FROM ldbc.Person AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.id
JOIN ldbc.Person AS v2 ON v1.__cg_end_0 = v2.id
WHERE v0.id = 14
ORDER BY v1.creationDate DESC NULLS FIRST, bigint(v2.id) ASC NULLS LAST
