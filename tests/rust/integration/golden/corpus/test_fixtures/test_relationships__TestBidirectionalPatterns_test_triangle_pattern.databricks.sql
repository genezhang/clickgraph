SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`, 
      v4.name AS `c.name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
JOIN test_integration.follows AS v5 ON v5.follower_id = v4.user_id AND v5.followed_id = v0.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND (v1.follower_id <> v5.follower_id OR v1.followed_id <> v5.followed_id) AND (v3.follower_id <> v5.follower_id OR v3.followed_id <> v5.followed_id))
