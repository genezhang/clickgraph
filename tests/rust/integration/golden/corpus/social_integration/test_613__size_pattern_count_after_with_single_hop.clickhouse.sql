WITH with_w1 AS (
SELECT 
      v0.user_id AS "v1__user_id", 
      v0.user_id AS "p2_v1_user_id"
FROM test_integration.users_test AS v0
), 
subquery_s2 AS (
SELECT 
      v2.follower_id AS "v1__user_id", 
      count(*) AS "__value"
FROM test_integration.user_follows_test AS v2
JOIN test_integration.users_test AS v3 ON v2.followed_id = v3.user_id
GROUP BY v2.follower_id
)
SELECT 
      w1.p2_v1_user_id AS "a.user_id", 
      coalesce(s2.__value, 0) AS "c"
FROM with_w1 AS w1
LEFT JOIN subquery_s2 AS s2 ON w1.v1__user_id = s2.v1__user_id
