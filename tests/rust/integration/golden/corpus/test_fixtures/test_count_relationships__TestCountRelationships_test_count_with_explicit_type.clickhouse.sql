SELECT 
      count(v1.follower_id) AS "total"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id

-- column total: Value [("total", "total")]