SELECT 
      v0.name AS "a.name", 
      v2.name AS "hub.name", 
      v4.name AS "c.name", 
      v8.name AS "d.name", 
      v5.name AS "e.name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
CROSS JOIN test_integration.users AS v5
JOIN test_integration.follows AS v6 ON v6.follower_id = v5.user_id AND v6.followed_id = v2.user_id
JOIN test_integration.follows AS v7 ON v7.follower_id = v2.user_id
JOIN test_integration.users AS v8 ON v7.followed_id = v8.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND (v1.follower_id <> v6.follower_id OR v1.followed_id <> v6.followed_id) AND (v1.follower_id <> v7.follower_id OR v1.followed_id <> v7.followed_id) AND (v3.follower_id <> v6.follower_id OR v3.followed_id <> v6.followed_id) AND (v3.follower_id <> v7.follower_id OR v3.followed_id <> v7.followed_id) AND (v6.follower_id <> v7.follower_id OR v6.followed_id <> v7.followed_id) AND v2.user_id = 2)

-- column a.name: Value [("a.name", "a.name")]
-- column hub.name: Value [("hub.name", "hub.name")]
-- column c.name: Value [("c.name", "c.name")]
-- column d.name: Value [("d.name", "d.name")]
-- column e.name: Value [("e.name", "e.name")]