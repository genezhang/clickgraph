SELECT 
      v0.name AS "a.name", 
      count(v2.user_id) AS "follower_count"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.followed_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.follower_id = v2.user_id
GROUP BY v0.name
ORDER BY count(v2.user_id) DESC NULLS FIRST, v0.name ASC

-- column a.name: Value [("a.name", "a.name")]
-- column follower_count: Value [("follower_count", "follower_count")]