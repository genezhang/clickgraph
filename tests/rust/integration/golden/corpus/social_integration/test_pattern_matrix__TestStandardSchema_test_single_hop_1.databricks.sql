SELECT 
      v0.city AS `a.city`, 
      v2.city AS `b.city`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LIMIT 10
-- column a.city: Value [("a.city", "a.city")]
-- column b.city: Value [("b.city", "b.city")]