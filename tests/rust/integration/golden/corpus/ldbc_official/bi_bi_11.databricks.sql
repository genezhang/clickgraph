WITH v5_both AS (
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
), 
with_w2 AS (
SELECT DISTINCT 
      v4.id AS `v7__id`, 
      v0.id AS `v8__id`, 
      v6.id AS `v9__id`, 
      v6.id AS `p2_v9_id`
FROM ldbc.Person AS v0
JOIN ldbc.Person_isLocatedIn_Place AS v1 ON v1.PersonId = v0.id
JOIN ldbc.Place AS v2 ON v1.CityId = v2.id
JOIN ldbc.Place_isPartOf_Place AS v3 ON v3.Place1Id = v2.id
JOIN ldbc.Place AS v4 ON v3.Place2Id = v4.id
JOIN v5_both AS v5 ON v5.__cg_start_0 = v0.id
JOIN ldbc.Person AS v6 ON v5.__cg_end_0 = v6.id
WHERE (((v2.type = 'City')) AND ((v4.type = 'Country')) AND v4.name = 'India' AND ((v0.id < v6.id AND 1262305305684 <= v5.creationDate) AND v5.creationDate <= 1354157106851))
), 
with_w3 AS (
SELECT DISTINCT 
      w2.v7__id AS `v13__id`, 
      w2.v8__id AS `v14__id`, 
      w2.v9__id AS `v15__id`, 
      w2.p2_v9_id AS `p3_v15_id`
FROM with_w2 AS w2
JOIN ldbc.Person_isLocatedIn_Place AS v10 ON v10.PersonId = w2.v9__id
JOIN ldbc.Place AS v11 ON v10.CityId = v11.id
JOIN ldbc.Place_isPartOf_Place AS v12 ON v12.Place1Id = v11.id AND v12.Place2Id = w2.v7__id
WHERE ((v11.type = 'City'))
), 
v16_both AS (
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
), 
with_w5 AS (
SELECT DISTINCT 
      w3.v14__id AS `v21__id`, 
      w3.v15__id AS `v22__id`, 
      v17.id AS `v23__id`
FROM with_w3 AS w3
JOIN v16_both AS v16 ON v16.__cg_start_0 = w3.v15__id
JOIN ldbc.Person AS v17 ON v16.__cg_end_0 = v17.id
JOIN ldbc.Person_isLocatedIn_Place AS v18 ON v18.PersonId = v17.id
JOIN ldbc.Place AS v19 ON v18.CityId = v19.id
JOIN ldbc.Place_isPartOf_Place AS v20 ON v20.Place1Id = v19.id AND v20.Place2Id = w3.v13__id
WHERE (((v19.type = 'City')) AND ((w3.p3_v15_id < v17.id AND 1262305305684 <= v16.creationDate) AND v16.creationDate <= 1354157106851))
), 
v24_both AS (
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
), 
with_w7 AS (
SELECT DISTINCT 
      w5.v21__id AS `v25__id`, 
      w5.v22__id AS `v26__id`, 
      w5.v23__id AS `v27__id`
FROM with_w5 AS w5
JOIN v24_both AS v24 ON v24.__cg_start_0 = w5.v23__id AND v24.__cg_end_0 = w5.v21__id
WHERE (1262305305684 <= v24.creationDate AND v24.creationDate <= 1354157106851)
)
SELECT 
      count(*) AS `count`
FROM with_w7 AS w7

-- column count: Value [("count", "count")]