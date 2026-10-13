SELECT DISTINCT 
      v0.user_id AS "a.user_id", 
      v2.user_id AS "b.user_id", 
      v4.user_id AS "c.user_id"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND v0.user_id = 1)

-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.user_id: Value [("b.user_id", "b.user_id")]
-- column c.user_id: Value [("c.user_id", "c.user_id")]