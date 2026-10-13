SELECT 
      v0.user_id AS "a.user_id"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id AND v1.followed_id = v0.user_id
WHERE (v0.name = 'Alice' AND v0.user_id = 1)

-- column a.user_id: Value [("a.user_id", "a.user_id")]