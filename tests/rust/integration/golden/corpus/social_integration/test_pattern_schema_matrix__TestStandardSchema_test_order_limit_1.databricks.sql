SELECT 
      v0.post_id AS `n.post_id`
FROM test_integration.posts_test AS v0
WHERE v0.post_id IS NOT NULL
ORDER BY v0.post_id DESC NULLS FIRST
LIMIT 10 OFFSET 5