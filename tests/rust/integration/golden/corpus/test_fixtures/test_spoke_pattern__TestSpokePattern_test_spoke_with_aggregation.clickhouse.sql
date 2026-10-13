SELECT 
      v2.name AS "hub.name", 
      count(v0.user_id) AS "follower_count"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v2.user_id = 2
GROUP BY v2.name

-- column hub.name: Value [("hub.name", "hub.name")]
-- column follower_count: Value [("follower_count", "follower_count")]