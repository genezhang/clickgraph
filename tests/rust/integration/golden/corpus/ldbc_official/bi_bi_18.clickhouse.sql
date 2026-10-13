WITH v3_both AS (
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
v5_both AS (
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
v8_both AS (
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
subquery_s4 AS (
SELECT 
      v2.id AS "v2__id", 
      v6.id AS "v6__id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM ldbc.Person AS v2
JOIN v8_both AS v8 ON v8.__cg_start_0 = v2.id
JOIN ldbc.Person AS v6 ON v8.__cg_end_0 = v6.id
WHERE v2.id <> v6.id
GROUP BY v2.id, v6.id
)
SELECT 
      v2.id AS "person1Id", 
      v6.id AS "person2Id", 
      count(DISTINCT v4.id) AS "mutualFriendCount"
FROM ldbc.Tag AS v0
JOIN ldbc.Person_hasInterest_Tag AS v1 ON v1.TagId = v0.id
JOIN ldbc.Person AS v2 ON v1.PersonId = v2.id
JOIN v3_both AS v3 ON v3.__cg_start_0 = v2.id
JOIN ldbc.Person AS v4 ON v3.__cg_end_0 = v4.id
JOIN v5_both AS v5 ON v5.__cg_start_0 = v4.id
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
JOIN ldbc.Person_hasInterest_Tag AS v7 ON v7.PersonId = v6.id AND v7.TagId = v0.id
LEFT JOIN subquery_s4 AS s4 ON v2.id = s4.v2__id AND v6.id = s4.v6__id
WHERE (v0.name = 'Hamid_Karzai' AND (v1.PersonId <> v7.PersonId OR v1.TagId <> v7.TagId) AND (v3.Person1Id <> v5.Person1Id OR v3.Person2Id <> v5.Person2Id) AND v2.id <> v6.id AND NOT coalesce(s4.__value, false))
GROUP BY v2.id, v6.id
ORDER BY count(DISTINCT v4.id) DESC NULLS FIRST, v2.id ASC, v6.id ASC
LIMIT 20
-- column person1Id: Value [("person1Id", "person1Id")]
-- column person2Id: Value [("person2Id", "person2Id")]
-- column mutualFriendCount: Value [("mutualFriendCount", "mutualFriendCount")]