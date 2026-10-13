SELECT 
      v0.user_id AS "a.user_id", 
      v2.post_date AS "b.created_at"
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LIMIT 10
-- column a.user_id: Value [("a.user_id", "a.user_id")]
-- column b.created_at: Value [("b.created_at", "b.created_at")]