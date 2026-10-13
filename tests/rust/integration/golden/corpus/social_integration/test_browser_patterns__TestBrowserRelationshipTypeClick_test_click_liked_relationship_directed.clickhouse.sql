SELECT 
      v1.user_id AS "r.from_id", 
      v1.post_id AS "r.to_id", 
      v1.like_date AS "r.like_date"
FROM test_integration.users_test AS v0
JOIN test_integration.post_likes_test AS v1 ON v1.user_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LIMIT 25