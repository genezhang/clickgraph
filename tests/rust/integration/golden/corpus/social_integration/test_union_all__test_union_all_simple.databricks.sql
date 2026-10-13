WITH with_w1 AS (
SELECT 
      v0.full_name AS `__cg_w1_c0`
FROM test_integration.users_test AS v0
WHERE v0.user_id = 1
), 
with_w2 AS (
SELECT 
      v2.full_name AS `__cg_w2_c0`
FROM test_integration.users_test AS v2
WHERE v2.user_id = 2
), 
with_w3 AS (
SELECT 
      CAST(w1.__cg_w1_c0 AS VARIANT) AS `__cg_c0`
FROM with_w1 AS w1

UNION ALL
SELECT 
      CAST(w2.__cg_w2_c0 AS VARIANT) AS `__cg_c0`
FROM with_w2 AS w2
)
SELECT 
      w3.__cg_c0 AS `name`
FROM with_w3 AS w3

-- column name: Value [("name", "name")]