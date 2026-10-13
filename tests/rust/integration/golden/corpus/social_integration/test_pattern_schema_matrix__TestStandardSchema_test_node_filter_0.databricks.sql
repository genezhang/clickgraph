SELECT 
      v0.post_id AS `n.post_id`
FROM test_integration.posts_test AS v0
WHERE v0.post_id IS NOT NULL
LIMIT 10
-- column n.post_id: Value [("n.post_id", "n.post_id")]