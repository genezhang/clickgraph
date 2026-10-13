WITH with_w1 AS (
SELECT 
      v0.user_id AS `v1__user_id`, 
      v0.user_id AS `p2_v1_user_id`
FROM test_integration.users_test AS v0
), 
subquery_d2 AS (
SELECT DISTINCT 
      w1.v1__user_id AS `v1__user_id`
FROM with_w1 AS w1
), 
subquery_s3 AS (
SELECT 
      d2.v1__user_id AS `v1__user_id`, 
      count(*) AS `__value`
FROM subquery_d2 AS d2
JOIN test_integration.user_follows_test AS v2 ON v2.follower_id = d2.v1__user_id
JOIN test_integration.users_test AS v3 ON v2.followed_id = v3.user_id
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = v3.user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
WHERE v2.follow_id <> v4.follow_id
GROUP BY d2.v1__user_id
)
SELECT 
      w1.p2_v1_user_id AS `a.user_id`, 
      coalesce(s3.__value, 0) AS `c`
FROM with_w1 AS w1
LEFT JOIN subquery_s3 AS s3 ON w1.v1__user_id = s3.v1__user_id
