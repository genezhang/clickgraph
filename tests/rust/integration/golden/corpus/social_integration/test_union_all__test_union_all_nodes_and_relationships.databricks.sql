WITH with_w1 AS (
SELECT DISTINCT 
      'node' AS `__cg_w1_c0`, 
      v0.full_name AS `__cg_w1_c1`
FROM test_integration.users_test AS v0
WHERE v0.user_id = 1
), 
with_w2 AS (
SELECT DISTINCT 
      'relationship' AS `__cg_w2_c0`, 
      concat(v3.full_name, ' follows ', v5.full_name) AS `__cg_w2_c1`
FROM test_integration.users_test AS v3
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = v3.user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
WHERE v3.user_id = 1
), 
with_w3 AS (
SELECT 
      CAST(w1.__cg_w1_c0 AS VARIANT) AS `__cg_c0`, 
      CAST(w1.__cg_w1_c1 AS VARIANT) AS `__cg_c1`
FROM with_w1 AS w1

UNION ALL
SELECT 
      CAST(w2.__cg_w2_c0 AS VARIANT) AS `__cg_c0`, 
      CAST(w2.__cg_w2_c1 AS VARIANT) AS `__cg_c1`
FROM with_w2 AS w2
)
SELECT 
      w3.__cg_c0 AS `entity`, 
      w3.__cg_c1 AS `name`
FROM with_w3 AS w3
