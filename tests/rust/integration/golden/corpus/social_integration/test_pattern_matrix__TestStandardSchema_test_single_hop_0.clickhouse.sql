SELECT 
      v0.is_active AS "a.is_active", 
      v2.is_active AS "b.is_active"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LIMIT 10
-- column a.is_active: Value [("a.is_active", "a.is_active")]
-- column b.is_active: Value [("b.is_active", "b.is_active")]