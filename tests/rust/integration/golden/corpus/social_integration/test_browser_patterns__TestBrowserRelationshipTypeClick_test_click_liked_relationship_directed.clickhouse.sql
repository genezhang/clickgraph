SELECT 
      r.user_id AS "r.from_id", 
      r.post_id AS "r.to_id", 
      r.like_date AS "r.like_date"
FROM test_integration.post_likes_test AS r
LIMIT 25