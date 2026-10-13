SELECT 
      v0.author_id AS `n.author_id`, 
      v0.post_content AS `n.content`, 
      v0.post_date AS `n.created_at`, 
      v0.post_id AS `n.post_id`, 
      v0.post_title AS `n.title`
FROM test_integration.posts_test AS v0
LIMIT 5