SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`, 
      v4.name AS `c.name`, 
      v6.name AS `d.name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
JOIN test_integration.follows AS v5 ON v5.follower_id = v4.user_id
JOIN test_integration.users AS v6 ON v5.followed_id = v6.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND (v1.follower_id <> v5.follower_id OR v1.followed_id <> v5.followed_id) AND (v3.follower_id <> v5.follower_id OR v3.followed_id <> v5.followed_id))

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column c.name: Value [("c.name", "c.name")]
-- column d.name: Value [("d.name", "d.name")]