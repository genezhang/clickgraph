SELECT 
      v0.user_id AS "u.user_id", 
      count(v1.follow_id) AS "follow_count"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.user_id
ORDER BY count(v1.follow_id) DESC NULLS FIRST
LIMIT 10
-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column follow_count: Value [("follow_count", "follow_count")]