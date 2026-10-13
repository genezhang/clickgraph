WITH v1_both AS (
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person1Id AS "__cg_start_0", 
      e.Person2Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person2Id AS "__cg_start_0", 
      e.Person1Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
with_w2 AS (
SELECT DISTINCT 
      v6.id AS "v7__id", 
      v6.name AS "p2_v7_name", 
      v4.id AS "v8__id", 
      v4.creationDate AS "p2_v8_creationDate"
FROM ldbc.Person AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.id
JOIN ldbc.Person AS v2 ON v1.__cg_end_0 = v2.id
JOIN ldbc.Post_hasCreator_Person AS v3 ON v3.PersonId = v2.id
JOIN ldbc.Post AS v4 ON v3.PostId = v4.id
JOIN ldbc.Post_hasTag_Tag AS v5 ON v5.PostId = v4.id
JOIN ldbc.Tag AS v6 ON v5.TagId = v6.id
WHERE v0.id = 14
), 
with_w3 AS (
SELECT 
      w2.v7__id AS "v9__id", 
      w2.p2_v7_name AS "p2_v9_name", 
      CASE WHEN (1354157566475 > w2.p2_v8_creationDate AND w2.p2_v8_creationDate >= 1262440406808) THEN 1 ELSE 0 END AS "v10", 
      CASE WHEN 1262440406808 > w2.p2_v8_creationDate THEN 1 ELSE 0 END AS "v11"
FROM with_w2 AS w2
), 
with_w4 AS (
SELECT 
      w3.v9__id AS "v12__id", 
      w3.p2_v9_name AS "p3_v12_name", 
      coalesce(sum(w3.v10), 0) AS "v13", 
      coalesce(sum(w3.v11), 0) AS "v14"
FROM with_w3 AS w3
GROUP BY w3.v9__id, w3.p2_v9_name
HAVING (coalesce(sum(w3.v10), 0) > 0 AND coalesce(sum(w3.v11), 0) = 0)
)
SELECT 
      w4.p3_v12_name AS "tagName", 
      w4.v13 AS "postCount"
FROM with_w4 AS w4
ORDER BY w4.v13 DESC NULLS FIRST, w4.p3_v12_name ASC
LIMIT 10
-- column tagName: Value [("tagName", "tagName")]
-- column postCount: Value [("postCount", "postCount")]