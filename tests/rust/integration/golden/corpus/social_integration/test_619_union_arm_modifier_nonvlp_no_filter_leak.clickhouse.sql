WITH with_w1 AS (
SELECT 
      v2.user_id AS "__cg_w1_c0", 
      v2.user_id AS "__cg_w1_o0"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v0.user_id = 5
ORDER BY v2.user_id ASC
LIMIT 2), 
with_w2 AS (
SELECT 
      v4.user_id AS "__cg_w2_c0"
FROM test_integration.users_test AS v4
WHERE v4.user_id = 6
), 
with_w3 AS (
SELECT 
      CAST(w1.__cg_w1_c0, 'Dynamic') AS "__cg_c0", 
      0 AS "__cg_arm", 
      row_number() OVER (ORDER BY w1.__cg_w1_o0 ASC) AS "__cg_row"
FROM with_w1 AS w1

UNION ALL
SELECT 
      CAST(w2.__cg_w2_c0, 'Dynamic') AS "__cg_c0", 
      1 AS "__cg_arm", 
      0 AS "__cg_row"
FROM with_w2 AS w2
)
SELECT 
      w3.__cg_c0 AS "id"
FROM with_w3 AS w3
ORDER BY w3.__cg_arm ASC, w3.__cg_row ASC
