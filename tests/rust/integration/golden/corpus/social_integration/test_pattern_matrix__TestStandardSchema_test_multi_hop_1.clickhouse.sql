SELECT 
      v0.email_address AS "a.email", 
      v4.email_address AS "c.email"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v1.follow_id <> v3.follow_id
LIMIT 5
-- column a.email: Value [("a.email", "a.email")]
-- column c.email: Value [("c.email", "c.email")]