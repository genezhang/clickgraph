WITH subquery_s1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v1.follower_id
)
SELECT 
      v0.full_name AS "a.name"
FROM test_integration.users_test AS v0
LEFT JOIN subquery_s1 AS s1 ON v0.user_id = s1.v0__user_id
WHERE coalesce(s1.__value, false)

-- column a.name: Value [("a.name", "a.name")]