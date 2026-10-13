WITH with_w1 AS (
SELECT 
      v0.email_address AS `v3`, 
      count(v1.follow_id) AS `v4`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.email_address
)
SELECT 
      w1.v3 AS `prop`, 
      w1.v4 AS `cnt`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10
-- column prop: Value [("prop", "prop")]
-- column cnt: Value [("cnt", "cnt")]