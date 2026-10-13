WITH optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.user_id AS "p2_v2_user_id"
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v0.is_active AS "p2_v0_is_active", 
      v0.user_id AS "p2_v0_user_id", 
      o1.v2__user_id AS "v2__user_id", 
      o1.p2_v2_user_id AS "p2_v2_user_id"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
), 
optional_d3 AS (
SELECT DISTINCT 
      w2.p2_v0_is_active AS "p2_v0_is_active", 
      w2.v0__user_id AS "v0__user_id", 
      w2.v2__user_id AS "v2__user_id"
FROM with_w2 AS w2
), 
optional_o4 AS (
SELECT 
      d3.v2__user_id AS "v2__user_id", 
      d3.v0__user_id AS "v0__user_id", 
      v4.user_id AS "v4__user_id", 
      v4.user_id AS "p2_v4_user_id"
FROM optional_d3 AS d3
JOIN test_integration.user_follows_test AS v3 ON v3.followed_id = d3.v2__user_id
JOIN test_integration.users_test AS v4 ON v3.follower_id = v4.user_id
WHERE (d3.v2__user_id IS NOT NULL AND d3.p2_v0_is_active = true)
)
SELECT 
      w2.p2_v0_user_id AS "a.user_id", 
      w2.p2_v2_user_id AS "b.user_id", 
      o4.p2_v4_user_id AS "c.user_id"
FROM with_w2 AS w2
LEFT JOIN optional_o4 AS o4 ON w2.v2__user_id = o4.v2__user_id AND w2.v0__user_id = o4.v0__user_id
ORDER BY w2.p2_v0_user_id ASC, w2.p2_v2_user_id ASC, o4.p2_v4_user_id ASC

-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]
-- column c.user_id: Value [("c.user_id", "c.user_id")]