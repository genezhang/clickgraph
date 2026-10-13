SELECT 
      v0.name AS "a.name", 
      v2.name AS "hub.name", 
      v3.name AS "c.name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v4 ON v4.followed_id = v2.user_id
JOIN test_integration.users AS v3 ON v4.follower_id = v3.user_id
WHERE ((v1.follower_id <> v4.follower_id OR v1.followed_id <> v4.followed_id) AND v2.user_id = 2)
