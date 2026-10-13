WITH subquery_s1 AS (
SELECT 
      v3.follower_id AS "v2__user_id", 
      count(*) AS "__value"
FROM test_integration.user_follows_test AS v3
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v3.follower_id IS NOT NULL
GROUP BY v3.follower_id
), 
optional_o1 AS (
SELECT 
      v1.follower_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.id AS "p2_v2_id"
FROM test_integration.user_follows_test AS v1
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LEFT JOIN subquery_s1 AS s1 ON v2.user_id = s1.v2__user_id
WHERE coalesce(s1.__value, 0) > 0
)
SELECT 
      v0.id AS "a.id", 
      o1.p2_v2_id AS "b.id"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
