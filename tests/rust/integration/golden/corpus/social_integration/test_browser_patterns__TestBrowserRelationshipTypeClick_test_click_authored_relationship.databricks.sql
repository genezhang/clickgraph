SELECT 
      r.author_id AS `r.from_id`, 
      r.post_id AS `r.to_id`
FROM test_integration.posts_test AS r
LIMIT 25