SELECT 
      b.author_id AS "a.user_id", 
      b.post_date AS "b.created_at"
FROM test_integration.posts_test AS b
LIMIT 10