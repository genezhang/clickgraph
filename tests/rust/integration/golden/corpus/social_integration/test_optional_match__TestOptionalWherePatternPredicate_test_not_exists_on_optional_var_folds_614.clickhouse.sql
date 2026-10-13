WITH subquery_s1 AS (
SELECT 
      v3.follower_id AS "v2__user_id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM test_integration.user_follows_test AS v3
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v3.follower_id IS NOT NULL
GROUP BY v3.follower_id
), 
optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.user_id AS "p2_v2_user_id"
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LEFT JOIN subquery_s1 AS s1 ON v2.user_id = s1.v2__user_id
WHERE NOT coalesce(s1.__value, false)
)
SELECT 
      v0.user_id AS "a.user_id", 
      o1.p2_v2_user_id AS "b.user_id"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
ORDER BY v0.user_id ASC, o1.p2_v2_user_id ASC

-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]