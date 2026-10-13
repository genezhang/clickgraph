WITH v0_labels AS (
SELECT 
      'Post' AS `__cg_label`, 
      e.post_id AS `__cg_id_0`, 
      NULL AS `p2_v0_user_id`
FROM test_integration.posts_test AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.user_id AS `p2_v0_user_id`
FROM test_integration.users_test AS e
), 
with_w2 AS (
SELECT 
      'node' AS `__cg_w2_c0`, 
      v0.p2_v0_user_id AS `__cg_w2_c1`
FROM v0_labels AS v0
WHERE v0.p2_v0_user_id = 1
), 
with_w3 AS (
SELECT 
      'relationship' AS `__cg_w3_c0`, 
      v4.follow_date AS `__cg_w3_c1`
FROM test_integration.users_test AS v3
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = v3.user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
WHERE v4.follow_date IS NOT NULL
LIMIT 10), 
with_w4 AS (
SELECT 
      CAST(w2.__cg_w2_c0 AS VARIANT) AS `__cg_c0`, 
      CAST(w2.__cg_w2_c1 AS VARIANT) AS `__cg_c1`
FROM with_w2 AS w2

UNION ALL
SELECT 
      CAST(w3.__cg_w3_c0 AS VARIANT) AS `__cg_c0`, 
      CAST(w3.__cg_w3_c1 AS VARIANT) AS `__cg_c1`
FROM with_w3 AS w3
)
SELECT 
      w4.__cg_c0 AS `entity`, 
      w4.__cg_c1 AS `value`
FROM with_w4 AS w4

-- column entity: Value [("entity", "entity")]
-- column value: Value [("value", "value")]