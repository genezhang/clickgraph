SELECT 
      v2.user_id AS "follower.user_id", 
      v2.full_name AS "follower.name"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.followed_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.follower_id = v2.user_id
WHERE v0.user_id = 2
LIMIT 10
-- column follower.user_id: Value [("follower.user_id", "follower.user_id")]
-- column follower.name: Value [("follower.name", "follower.name")]