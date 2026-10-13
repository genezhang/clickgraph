SELECT 
      v2.user_id AS `neighbor.user_id`, 
      v2.full_name AS `neighbor.name`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
WHERE v0.user_id = 1
LIMIT 10
-- column neighbor.user_id: Value [("neighbor.user_id", "neighbor.user_id")]
-- column neighbor.name: Value [("neighbor.name", "neighbor.name")]